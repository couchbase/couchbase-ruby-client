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
#include <core/document_id.hxx>
#include <core/io/dns_client.hxx>
#include <core/io/dns_config.hxx>
#include <core/logger/logger.hxx>
#include <core/operations/management/cluster_developer_preview_enable.hxx>
#include <core/operations/management/collections_manifest_get.hxx>
#include <core/utils/connection_string.hxx>
#include <core/utils/unsigned_leb128.hxx>
#include <core/utils/url_codec.hxx>

#include <fmt/core.h>
#include <snappy.h>

#include <future>
#include <string>

#include <ruby.h>
#if defined(HAVE_RUBY_VERSION_H)
#include <ruby/version.h>
#endif
#include <ruby/thread.h>

#include "rcb_backend.hxx"
#include "rcb_exceptions.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{
VALUE
cb_Backend_collections_manifest_get(VALUE self, VALUE bucket_name, VALUE timeout)
{
    const auto& cluster = cb_backend_to_cluster(self);

    Check_Type(bucket_name, T_STRING);

    try {
        core::operations::management::collections_manifest_get_request req{ core::document_id{
          cb_string_new(bucket_name), "_default", "_default", "" } };
        cb_extract_timeout(req, timeout);
        auto promise = std::make_shared<std::promise<core::operations::management::collections_manifest_get_response>>();
        auto f = promise->get_future();
        cluster->execute(req, [promise](auto&& resp) {
            promise->set_value(std::forward<decltype(resp)>(resp));
        });
        auto resp = cb_wait_for_future(f);
        if (resp.ctx.ec()) {
            cb_throw_error(resp.ctx, fmt::format("unable to get collections manifest of the bucket \"{}\"", req.id.bucket()));
        }

        VALUE res = rb_hash_new();
        rb_hash_aset(res, rb_id2sym(rb_intern("uid")), ULL2NUM(resp.manifest.uid));
        VALUE scopes = rb_ary_new_capa(static_cast<long>(resp.manifest.scopes.size()));
        for (const auto& s : resp.manifest.scopes) {
            VALUE scope = rb_hash_new();
            rb_hash_aset(scope, rb_id2sym(rb_intern("uid")), ULL2NUM(s.uid));
            rb_hash_aset(scope, rb_id2sym(rb_intern("name")), cb_str_new(s.name));
            VALUE collections = rb_ary_new_capa(static_cast<long>(s.collections.size()));
            for (const auto& c : s.collections) {
                VALUE collection = rb_hash_new();
                rb_hash_aset(collection, rb_id2sym(rb_intern("uid")), ULL2NUM(c.uid));
                rb_hash_aset(collection, rb_id2sym(rb_intern("name")), cb_str_new(c.name));
                rb_ary_push(collections, collection);
            }
            rb_hash_aset(scope, rb_id2sym(rb_intern("collections")), collections);
            rb_ary_push(scopes, scope);
        }
        rb_hash_aset(res, rb_id2sym(rb_intern("scopes")), scopes);

        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

VALUE
cb_Backend_dns_srv(VALUE self, VALUE hostname, VALUE service)
{
    (void)self;
    Check_Type(hostname, T_STRING);
    Check_Type(service, T_SYMBOL);

    bool tls = false;

    if (ID type = rb_sym2id(service); type == rb_intern("couchbase")) {
        tls = false;
    } else if (type == rb_intern("couchbases")) {
        tls = true;
    } else {
        rb_raise(rb_eArgError, "Unsupported service type: %+" PRIsVALUE, service);
        return Qnil;
    }

    try {
        asio::io_context ctx;

        core::io::dns::dns_client client(ctx);
        std::string host_name = cb_string_new(hostname);
        std::string service_name("_couchbase");
        if (tls) {
            service_name = "_couchbases";
        }
        auto promise = std::make_shared<std::promise<core::io::dns::dns_srv_response>>();
        auto f = promise->get_future();
        client.query_srv(host_name, service_name, core::io::dns::dns_config::system_config(), [promise](auto&& resp) {
            promise->set_value(std::forward<decltype(resp)>(resp));
        });
        ctx.run();
        auto resp = cb_wait_for_future(f);
        if (resp.ec) {
            cb_throw_error_code(resp.ec, fmt::format("DNS SRV query failure for name \"{}\" (service: {})", host_name, service_name));
        }

        VALUE res = rb_ary_new();
        for (const auto& target : resp.targets) {
            VALUE addr = rb_hash_new();
            rb_hash_aset(addr, rb_id2sym(rb_intern("hostname")), cb_str_new(target.hostname));
            rb_hash_aset(addr, rb_id2sym(rb_intern("port")), UINT2NUM(target.port));
            rb_ary_push(res, addr);
        }
        return res;
    } catch (const std::system_error& se) {
        rb_exc_raise(cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

VALUE
cb_Backend_parse_connection_string(VALUE self, VALUE connection_string)
{
    (void)self;
    Check_Type(connection_string, T_STRING);

    std::string input(RSTRING_PTR(connection_string), static_cast<std::size_t>(RSTRING_LEN(connection_string)));
    auto connstr = core::utils::parse_connection_string(input);

    VALUE res = rb_hash_new();
    if (!connstr.scheme.empty()) {
        rb_hash_aset(res, rb_id2sym(rb_intern("scheme")), cb_str_new(connstr.scheme));
        rb_hash_aset(res, rb_id2sym(rb_intern("tls")), connstr.tls ? Qtrue : Qfalse);
    }

    VALUE nodes = rb_ary_new_capa(static_cast<long>(connstr.bootstrap_nodes.size()));
    for (const auto& entry : connstr.bootstrap_nodes) {
        VALUE node = rb_hash_new();
        rb_hash_aset(node, rb_id2sym(rb_intern("address")), cb_str_new(entry.address));
        if (entry.port > 0) {
            rb_hash_aset(node, rb_id2sym(rb_intern("port")), UINT2NUM(entry.port));
        }
        switch (entry.mode) {
            case core::utils::connection_string::bootstrap_mode::gcccp:
                rb_hash_aset(node, rb_id2sym(rb_intern("mode")), rb_id2sym(rb_intern("gcccp")));
                break;
            case core::utils::connection_string::bootstrap_mode::http:
                rb_hash_aset(node, rb_id2sym(rb_intern("mode")), rb_id2sym(rb_intern("http")));
                break;
            case core::utils::connection_string::bootstrap_mode::unspecified:
                break;
        }
        switch (entry.type) {
            case core::utils::connection_string::address_type::ipv4:
                rb_hash_aset(node, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("ipv4")));
                break;
            case core::utils::connection_string::address_type::ipv6:
                rb_hash_aset(node, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("ipv6")));
                break;
            case core::utils::connection_string::address_type::dns:
                rb_hash_aset(node, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("dns")));
                break;
        }
        rb_ary_push(nodes, node);
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("nodes")), nodes);

    VALUE params = rb_hash_new();
    for (const auto& [name, value] : connstr.params) {
        rb_hash_aset(params, cb_str_new(name), cb_str_new(value));
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("params")), params);

    if (connstr.default_bucket_name) {
        rb_hash_aset(res, rb_id2sym(rb_intern("default_bucket_name")), cb_str_new(connstr.default_bucket_name.value()));
    }
    if (connstr.default_port > 0) {
        rb_hash_aset(res, rb_id2sym(rb_intern("default_port")), UINT2NUM(connstr.default_port));
    }
    switch (connstr.default_mode) {
        case core::utils::connection_string::bootstrap_mode::gcccp:
            rb_hash_aset(res, rb_id2sym(rb_intern("default_mode")), rb_id2sym(rb_intern("gcccp")));
            break;
        case core::utils::connection_string::bootstrap_mode::http:
            rb_hash_aset(res, rb_id2sym(rb_intern("default_mode")), rb_id2sym(rb_intern("http")));
            break;
        case core::utils::connection_string::bootstrap_mode::unspecified:
            break;
    }
    if (connstr.error) {
        rb_hash_aset(res, rb_id2sym(rb_intern("error")), cb_str_new(connstr.error.value()));
    }
    return res;
}

VALUE
cb_Backend_snappy_compress(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);

    std::string compressed{};
    std::size_t compressed_size = snappy::Compress(RSTRING_PTR(data), static_cast<std::size_t>(RSTRING_LEN(data)), &compressed);

    return rb_external_str_new(compressed.data(), static_cast<long>(compressed_size));
}

VALUE
cb_Backend_snappy_uncompress(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);

    std::string uncompressed{};
    if (bool success = snappy::Uncompress(RSTRING_PTR(data), static_cast<std::size_t>(RSTRING_LEN(data)), &uncompressed); success) {
        return cb_str_new(uncompressed);
    }
    rb_raise(rb_eArgError, "Unable to decompress buffer");
    return Qnil;
}

VALUE
cb_Backend_leb128_encode(VALUE self, VALUE number)
{
    (void)self;
    switch (TYPE(number)) {
        case T_FIXNUM:
        case T_BIGNUM:
            break;
        default:
            rb_raise(rb_eArgError, "The value must be a number");
    }
    core::utils::unsigned_leb128<std::uint64_t> encoded(NUM2ULL(number));
    return cb_str_new(encoded.data(), encoded.size());
}

VALUE
cb_Backend_leb128_decode(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);
    auto buf = cb_binary_new(data);
    if (buf.empty()) {
        rb_raise(rb_eArgError, "Unable to decode the buffer as LEB128: the buffer is empty");
    }

    auto [value, rest] = core::utils::decode_unsigned_leb128<std::uint64_t>(buf, core::utils::leb_128_no_throw());
    if (rest.data() != nullptr) {
        return ULL2NUM(value);
    }
    rb_raise(rb_eArgError, "Unable to decode the buffer as LEB128");
    return Qnil;
}

VALUE
cb_Backend_query_escape(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);
    auto encoded = core::utils::string_codec::v2::query_escape(cb_string_new(data));
    return cb_str_new(encoded);
}

VALUE
cb_Backend_path_escape(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_STRING);
    auto encoded = core::utils::string_codec::v2::path_escape(cb_string_new(data));
    return cb_str_new(encoded);
}

int
cb_for_each_form_encode_value(VALUE key, VALUE value, VALUE arg)
{
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* values = reinterpret_cast<std::map<std::string, std::string>*>(arg);
    VALUE key_str = rb_obj_as_string(key);
    VALUE value_str = rb_obj_as_string(value);
    values->emplace(cb_string_new(key_str), cb_string_new(value_str));
    return ST_CONTINUE;
}

VALUE
cb_Backend_form_encode(VALUE self, VALUE data)
{
    (void)self;
    Check_Type(data, T_HASH);
    std::map<std::string, std::string> values{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    rb_hash_foreach(data, cb_for_each_form_encode_value, reinterpret_cast<VALUE>(&values));
    auto encoded = core::utils::string_codec::v2::form_encode(values);
    return cb_str_new(encoded);
}

VALUE
cb_Backend_cluster_enable_developer_preview(VALUE self)
{
    const auto& cluster = couchbase::ruby::cb_backend_to_cluster(self);

    try {
        couchbase::core::operations::management::cluster_developer_preview_enable_request req{};
        auto promise = std::make_shared<std::promise<couchbase::core::operations::management::cluster_developer_preview_enable_response>>();
        auto f = promise->get_future();
        cluster->execute(req, [promise](auto&& resp) {
            promise->set_value(std::forward<decltype(resp)>(resp));
        });

        if (auto resp = couchbase::ruby::cb_wait_for_future(f); resp.ctx.ec) {
            couchbase::ruby::cb_throw_error(resp.ctx, "unable to enable developer preview for this cluster");
        }
        CB_LOG_CRITICAL_RAW(
          "Developer preview cannot be disabled once it is enabled. If you enter developer preview mode you will not be able to "
          "upgrade. DO NOT USE IN PRODUCTION.");
        return Qtrue;
    } catch (const std::system_error& se) {
        rb_exc_raise(couchbase::ruby::cb_map_error_code(se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
    } catch (const couchbase::ruby::ruby_exception& e) {
        rb_exc_raise(e.exception_object());
    }
    return Qnil;
}

} // namespace

void
init_extras(VALUE cBackend)
{
    /* utility function that are not intended for public usage */
    rb_define_method(cBackend, "collections_manifest_get", cb_Backend_collections_manifest_get, 2);
    rb_define_method(cBackend, "cluster_enable_developer_preview!", cb_Backend_cluster_enable_developer_preview, 0);

    rb_define_singleton_method(cBackend, "dns_srv", cb_Backend_dns_srv, 2);
    rb_define_singleton_method(cBackend, "parse_connection_string", cb_Backend_parse_connection_string, 1);
    rb_define_singleton_method(cBackend, "snappy_compress", cb_Backend_snappy_compress, 1);
    rb_define_singleton_method(cBackend, "snappy_uncompress", cb_Backend_snappy_uncompress, 1);
    rb_define_singleton_method(cBackend, "leb128_encode", cb_Backend_leb128_encode, 1);
    rb_define_singleton_method(cBackend, "leb128_decode", cb_Backend_leb128_decode, 1);
    rb_define_singleton_method(cBackend, "query_escape", cb_Backend_query_escape, 1);
    rb_define_singleton_method(cBackend, "path_escape", cb_Backend_path_escape, 1);
    rb_define_singleton_method(cBackend, "form_encode", cb_Backend_form_encode, 1);
}
} // namespace couchbase::ruby
