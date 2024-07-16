/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <core/cluster.hxx>
#include <core/logger/logger.hxx>
#include <core/utils/connection_string.hxx>

#include <asio/io_context.hpp>
#include <fmt/core.h>

#include <future>
#include <memory>
#include <thread>

#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_exceptions.hxx"
#include "rcb_logger.hxx"
#include "rcb_utils.hxx"
#include "rcb_version.hxx"

namespace couchbase::ruby
{
namespace
{
struct cb_backend_data {
    std::unique_ptr<asio::io_context> ctx;
    std::shared_ptr<core::cluster> cluster;
    std::thread worker;
};

void
cb_extract_option_milliseconds(std::chrono::milliseconds& field, VALUE options, const char* name)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
        if (NIL_P(val)) {
            return;
        }
        switch (TYPE(val)) {
            case T_FIXNUM:
                field = std::chrono::milliseconds(FIX2ULONG(val));
                break;
            case T_BIGNUM:
                field = std::chrono::milliseconds(NUM2ULL(val));
                break;
            default:
                throw ruby_exception(rb_eArgError,
                                     rb_sprintf("%s must be a Integer representing milliseconds, but given %+" PRIsVALUE, name, val));
        }
    }
}

void
cb_extract_dns_config(core::io::dns::dns_config& config, VALUE options)
{
    if (!NIL_P(options) && TYPE(options) == T_HASH) {
        return;
    }

    auto timeout{ core::timeout_defaults::dns_srv_timeout };
    cb_extract_option_milliseconds(timeout, options, "dns_srv_timeout");

    std::string nameserver{ core::io::dns::dns_config::default_nameserver };
    cb_extract_option_string(nameserver, options, "dns_srv_nameserver");

    std::uint16_t port{ core::io::dns::dns_config::default_port };
    cb_extract_option_number(port, options, "dns_srv_port");

    config = core::io::dns::dns_config(nameserver, port, timeout);
}

void
cb_backend_close(cb_backend_data* backend)
{
    if (backend->cluster) {
        auto promise = std::make_shared<std::promise<void>>();
        auto f = promise->get_future();
        backend->cluster->close([promise]() {
            promise->set_value();
        });
        f.wait();
        if (backend->worker.joinable()) {
            backend->worker.join();
        }
        backend->cluster.reset();
        backend->ctx.reset(nullptr);
    }
}

void
cb_Backend_mark(void* /* ptr */)
{
    /* no embedded ruby objects -- no mark */
}

void
cb_Backend_free(void* ptr)
{
    auto* backend = static_cast<cb_backend_data*>(ptr);
    cb_backend_close(backend);
    ruby_xfree(backend);
}

size_t
cb_Backend_memsize(const void* ptr)
{
    const auto* backend = static_cast<const cb_backend_data*>(ptr);
    return sizeof(*backend) + sizeof(*backend->cluster);
}

const rb_data_type_t cb_backend_type{
    "Couchbase/Backend",
    { cb_Backend_mark,
      cb_Backend_free,
      cb_Backend_memsize,
// only one reserved field when GC.compact implemented
#ifdef T_MOVED
      nullptr,
#endif
      {} },
#ifdef RUBY_TYPED_FREE_IMMEDIATELY
    nullptr,
    nullptr,
    RUBY_TYPED_FREE_IMMEDIATELY,
#endif
};

VALUE
cb_Backend_allocate(VALUE klass)
{
    cb_backend_data* backend = nullptr;
    VALUE obj = TypedData_Make_Struct(klass, cb_backend_data, &cb_backend_type, backend);
    backend->ctx = std::make_unique<asio::io_context>();
    backend->cluster = std::make_shared<core::cluster>(*backend->ctx);
    backend->worker = std::thread([backend]() {
        backend->ctx->run();
    });
    return obj;
}

VALUE
cb_Backend_open(VALUE self, VALUE connection_string, VALUE credentials, VALUE options)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(connection_string, T_STRING);
    Check_Type(credentials, T_HASH);

    VALUE username = Qnil;
    VALUE password = Qnil;

    VALUE certificate_path = rb_hash_aref(credentials, rb_id2sym(rb_intern("certificate_path")));
    VALUE key_path = rb_hash_aref(credentials, rb_id2sym(rb_intern("key_path")));
    if (NIL_P(certificate_path) || NIL_P(key_path)) {
        username = rb_hash_aref(credentials, rb_id2sym(rb_intern("username")));
        password = rb_hash_aref(credentials, rb_id2sym(rb_intern("password")));
        Check_Type(username, T_STRING);
        Check_Type(password, T_STRING);
    } else {
        Check_Type(certificate_path, T_STRING);
        Check_Type(key_path, T_STRING);
    }
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
    }

    try {
        auto input = cb_string_new(connection_string);
        auto connstr = core::utils::parse_connection_string(input);
        if (connstr.error) {
            throw ruby_exception(exc_invalid_argument(),
                                 fmt::format(R"(Failed to parse connection string "{}": {})", input, connstr.error.value()));
        }
        core::cluster_credentials auth{};
        if (NIL_P(certificate_path) || NIL_P(key_path)) {
            auth.username = cb_string_new(username);
            auth.password = cb_string_new(password);
            if (!NIL_P(options)) {
                VALUE allowed_mechanisms = rb_hash_aref(options, rb_id2sym(rb_intern("allowed_sasl_mechanisms")));
                if (!NIL_P(allowed_mechanisms)) {
                    cb_check_type(allowed_mechanisms, T_ARRAY);
                    auto allowed_mechanisms_size = static_cast<std::size_t>(RARRAY_LEN(allowed_mechanisms));
                    if (allowed_mechanisms_size < 1) {
                        throw ruby_exception(exc_invalid_argument(), "allowed_sasl_mechanisms list cannot be empty");
                    }
                    std::vector<std::string> mechanisms{};
                    mechanisms.reserve(allowed_mechanisms_size);
                    for (std::size_t i = 0; i < allowed_mechanisms_size; ++i) {
                        VALUE mechanism = rb_ary_entry(allowed_mechanisms, static_cast<long>(i));
                        if (mechanism == rb_id2sym(rb_intern("scram_sha512"))) {
                            mechanisms.emplace_back("SCRAM-SHA512");
                        } else if (mechanism == rb_id2sym(rb_intern("scram_sha256"))) {
                            mechanisms.emplace_back("SCRAM-SHA256");
                        } else if (mechanism == rb_id2sym(rb_intern("scram_sha1"))) {
                            mechanisms.emplace_back("SCRAM-SHA1");
                        } else if (mechanism == rb_id2sym(rb_intern("plain"))) {
                            mechanisms.emplace_back("PLAIN");
                        }
                    }
                    auth.allowed_sasl_mechanisms.emplace(mechanisms);
                }
            }
        } else {
            if (!connstr.tls) {
                throw ruby_exception(exc_invalid_argument(),
                                     "Certificate authenticator requires TLS connection, check the schema of the connection string");
            }
            auth.certificate_path = cb_string_new(certificate_path);
            auth.key_path = cb_string_new(key_path);
        }
        core::origin origin(auth, connstr);

        cb_extract_option_bool(origin.options().enable_tracing, options, "enable_tracing");
        if (origin.options().enable_tracing) {
            cb_extract_option_milliseconds(origin.options().tracing_options.orphaned_emit_interval, options, "orphaned_emit_interval");
            cb_extract_option_number(origin.options().tracing_options.orphaned_sample_size, options, "orphaned_sample_size");
            cb_extract_option_milliseconds(origin.options().tracing_options.threshold_emit_interval, options, "threshold_emit_interval");
            cb_extract_option_number(origin.options().tracing_options.threshold_sample_size, options, "threshold_sample_size");
            cb_extract_option_milliseconds(origin.options().tracing_options.key_value_threshold, options, "key_value_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.query_threshold, options, "query_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.view_threshold, options, "view_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.search_threshold, options, "search_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.analytics_threshold, options, "analytics_threshold");
            cb_extract_option_milliseconds(origin.options().tracing_options.management_threshold, options, "management_threshold");
        }
        cb_extract_option_bool(origin.options().enable_metrics, options, "enable_metrics");
        if (origin.options().enable_metrics) {
            cb_extract_option_milliseconds(origin.options().metrics_options.emit_interval, options, "metrics_emit_interval");
        }
        cb_extract_option_milliseconds(origin.options().bootstrap_timeout, options, "bootstrap_timeout");
        cb_extract_option_milliseconds(origin.options().resolve_timeout, options, "resolve_timeout");
        cb_extract_option_milliseconds(origin.options().connect_timeout, options, "connect_timeout");
        cb_extract_option_milliseconds(origin.options().key_value_timeout, options, "key_value_timeout");
        cb_extract_option_milliseconds(origin.options().key_value_durable_timeout, options, "key_value_durable_timeout");
        cb_extract_option_milliseconds(origin.options().view_timeout, options, "view_timeout");
        cb_extract_option_milliseconds(origin.options().query_timeout, options, "query_timeout");
        cb_extract_option_milliseconds(origin.options().analytics_timeout, options, "analytics_timeout");
        cb_extract_option_milliseconds(origin.options().search_timeout, options, "search_timeout");
        cb_extract_option_milliseconds(origin.options().management_timeout, options, "management_timeout");
        cb_extract_option_milliseconds(origin.options().tcp_keep_alive_interval, options, "tcp_keep_alive_interval");
        cb_extract_option_milliseconds(origin.options().config_poll_interval, options, "config_poll_interval");
        cb_extract_option_milliseconds(origin.options().config_poll_floor, options, "config_poll_floor");
        cb_extract_option_milliseconds(origin.options().config_idle_redial_timeout, options, "config_idle_redial_timeout");
        cb_extract_option_milliseconds(origin.options().idle_http_connection_timeout, options, "idle_http_connection_timeout");

        cb_extract_dns_config(origin.options().dns_config, options);

        cb_extract_option_number(origin.options().max_http_connections, options, "max_http_connections");

        cb_extract_option_bool(origin.options().enable_tls, options, "enable_tls");
        cb_extract_option_bool(origin.options().enable_mutation_tokens, options, "enable_mutation_tokens");
        cb_extract_option_bool(origin.options().enable_tcp_keep_alive, options, "enable_tcp_keep_alive");
        cb_extract_option_bool(origin.options().enable_dns_srv, options, "enable_dns_srv");
        cb_extract_option_bool(origin.options().show_queries, options, "show_queries");
        cb_extract_option_bool(origin.options().enable_unordered_execution, options, "enable_unordered_execution");
        cb_extract_option_bool(origin.options().enable_clustermap_notification, options, "enable_clustermap_notification");
        cb_extract_option_bool(origin.options().enable_compression, options, "enable_compression");

        cb_extract_option_string(origin.options().trust_certificate, options, "trust_certificate");
        cb_extract_option_string(origin.options().network, options, "network");

        VALUE proto = Qnil;
        cb_extract_option_symbol(proto, options, "use_ip_protocol");
        if (proto == rb_id2sym(rb_intern("any"))) {
            origin.options().use_ip_protocol = core::io::ip_protocol::any;
        } else if (proto == rb_id2sym(rb_intern("force_ipv4"))) {
            origin.options().use_ip_protocol = core::io::ip_protocol::force_ipv4;
        } else if (proto == rb_id2sym(rb_intern("force_ipv6"))) {
            origin.options().use_ip_protocol = core::io::ip_protocol::force_ipv6;
        } else if (!NIL_P(proto)) {
            throw ruby_exception(exc_invalid_argument(), "Failed to detect preferred IP protocol");
        }

        VALUE mode = Qnil;
        cb_extract_option_symbol(mode, options, "tls_verify");
        if (mode == rb_id2sym(rb_intern("none"))) {
            origin.options().tls_verify = core::tls_verify_mode::none;
        } else if (mode == rb_id2sym(rb_intern("peer"))) {
            origin.options().tls_verify = core::tls_verify_mode::peer;
        } else if (!NIL_P(mode)) {
            throw ruby_exception(exc_invalid_argument(), "Failed to select verification mode for TLS");
        }

        origin.options().user_agent_extra = user_agent_extra();

        auto promise = std::make_shared<std::promise<std::error_code>>();
        auto f = promise->get_future();
        cluster->open(origin, [promise](std::error_code ec) {
            promise->set_value(ec);
        });
        if (auto ec = cb_wait_for_future(f)) {
            cb_throw_error_code(ec, fmt::format("unable open cluster at {}", origin.next_address().first));
        }
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

VALUE
cb_Backend_close(VALUE self)
{
    cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);
    cb_backend_close(backend);
    flush_logger();
    return Qnil;
}

VALUE
cb_Backend_open_bucket(VALUE self, VALUE bucket, VALUE wait_until_ready)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket, T_STRING);
    bool wait = RTEST(wait_until_ready);

    try {
        std::string name(RSTRING_PTR(bucket), static_cast<std::size_t>(RSTRING_LEN(bucket)));

        if (wait) {
            auto promise = std::make_shared<std::promise<std::error_code>>();
            auto f = promise->get_future();
            cluster->open_bucket(name, [promise](std::error_code ec) {
                promise->set_value(ec);
            });
            if (auto ec = cb_wait_for_future(f)) {
                cb_throw_error_code(ec, fmt::format("unable open bucket \"{}\"", name));
            }
        } else {
            cluster->open_bucket(name, [name](std::error_code ec) {
                CB_LOG_WARNING("unable open bucket \"{}\": {}", name, ec.message());
            });
        }
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

} // namespace

VALUE
init_backend(VALUE mCouchbase)
{
    VALUE cBackend = rb_define_class_under(mCouchbase, "Backend", rb_cObject);
    rb_define_alloc_func(cBackend, cb_Backend_allocate);
    rb_define_method(cBackend, "open", cb_Backend_open, 3);
    rb_define_method(cBackend, "open_bucket", cb_Backend_open_bucket, 2);
    rb_define_method(cBackend, "close", cb_Backend_close, 0);
    return cBackend;
}

auto
cb_backend_to_cluster(VALUE self) -> const std::shared_ptr<core::cluster>&
{
    const cb_backend_data* backend = nullptr;
    TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

    if (!backend->cluster) {
        rb_raise(exc_cluster_closed(), "Cluster has been closed already");
    }
    return backend->cluster;
}

} // namespace couchbase::ruby
