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

#include <couchbase/cluster.hxx>

#include <couchbase/fork_event.hxx>
#include <couchbase/ip_protocol.hxx>

#include <core/cluster.hxx>
#include <core/logger/logger.hxx>
#include <core/utils/connection_string.hxx>

#include <asio/io_context.hpp>
#include <spdlog/fmt/bundled/core.h>

#include <future>
#include <list>
#include <memory>
#include <mutex>

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
  std::unique_ptr<cluster> instance{ nullptr };
};

class instance_registry
{
public:
  void add(cluster* instance)
  {
    std::scoped_lock lock(instances_mutex_);
    known_instances_.push_back(instance);
  }

  void remove(cluster* instance)
  {
    std::scoped_lock lock(instances_mutex_);
    known_instances_.remove(instance);
  }

  void notify_fork(couchbase::fork_event event)
  {
    if (event != couchbase::fork_event::prepare) {
      init_logger();
    }

    {
      std::scoped_lock lock(instances_mutex_);
      for (auto* instance : known_instances_) {
        instance->notify_fork(event);
      }
    }

    if (event == couchbase::fork_event::prepare) {
      flush_logger();
      couchbase::core::logger::shutdown();
    }
  }

private:
  std::mutex instances_mutex_;
  std::list<cluster*> known_instances_;
};

instance_registry instances;

VALUE
cb_Backend_notify_fork(VALUE self, VALUE event)
{
  static const auto id_prepare{ rb_intern("prepare") };
  static const auto id_parent{ rb_intern("parent") };
  static const auto id_child{ rb_intern("child") };

  try {
    cb_check_type(event, T_SYMBOL);

    if (rb_sym2id(event) == id_prepare) {
      instances.notify_fork(couchbase::fork_event::prepare);
    } else if (rb_sym2id(event) == id_parent) {
      instances.notify_fork(couchbase::fork_event::parent);
    } else if (rb_sym2id(event) == id_child) {
      instances.notify_fork(couchbase::fork_event::child);
    } else {
      throw ruby_exception(rb_eTypeError,
                           rb_sprintf("unexpected fork event type %" PRIsVALUE "", event));
    }
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }

  return Qnil;
}

void
cb_backend_close(cb_backend_data* backend)
{
  if (auto instance = std::move(backend->instance); instance) {
    instances.remove(instance.get());
    auto promise = std::make_shared<std::promise<void>>();
    auto f = promise->get_future();
    instance->close([promise = std::move(promise)]() mutable {
      promise->set_value();
    });
    f.wait();
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
  return sizeof(*backend);
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
  backend->instance = nullptr;
  return obj;
}

auto
construct_authenticator(VALUE credentials)
  -> std::variant<couchbase::password_authenticator, couchbase::certificate_authenticator>
{
  cb_check_type(credentials, T_HASH);

  static const auto sym_certificate_path{ rb_id2sym(rb_intern("certificate_path")) };
  static const auto sym_key_path{ rb_id2sym(rb_intern("key_path")) };

  const VALUE certificate_path = rb_hash_aref(credentials, sym_certificate_path);
  const VALUE key_path = rb_hash_aref(credentials, sym_key_path);

  if (NIL_P(certificate_path) || NIL_P(key_path)) {
    static const auto sym_username = rb_id2sym(rb_intern("username"));
    static const auto sym_password = rb_id2sym(rb_intern("password"));

    const VALUE username = rb_hash_aref(credentials, sym_username);
    const VALUE password = rb_hash_aref(credentials, sym_password);

    cb_check_type(username, T_STRING);
    cb_check_type(password, T_STRING);

    return couchbase::password_authenticator{
      cb_string_new(username),
      cb_string_new(password),
    };
  }

  cb_check_type(certificate_path, T_STRING);
  cb_check_type(key_path, T_STRING);

  return couchbase::certificate_authenticator{
    cb_string_new(certificate_path),
    cb_string_new(key_path),
  };
}

auto
construct_cluster_options(VALUE credentials, bool tls_enabled) -> couchbase::cluster_options
{
  std::variant<couchbase::password_authenticator, couchbase::certificate_authenticator>
    authenticator = construct_authenticator(credentials);

  if (std::holds_alternative<couchbase::password_authenticator>(authenticator)) {
    return couchbase::cluster_options{
      std::get<couchbase::password_authenticator>(std::move(authenticator)),
    };
  }

  if (!tls_enabled) {
    throw ruby_exception(
      exc_invalid_argument(),
      "Certificate authenticator requires TLS connection, check the connection string");
  }

  return couchbase::cluster_options{
    std::get<couchbase::certificate_authenticator>(std::move(authenticator)),
  };
}

auto
initialize_cluster_options(const core::utils::connection_string& connstr,
                           VALUE credentials,
                           VALUE options) -> couchbase::cluster_options
{
  auto cluster_options = construct_cluster_options(credentials, connstr.tls);
  cluster_options.behavior().append_to_user_agent(user_agent_extra());

  if (NIL_P(options)) {
    return cluster_options;
  }

  cb_check_type(options, T_HASH);

  static const auto sym_dns_srv_timeout = rb_id2sym(rb_intern("dns_srv_timeout"));
  if (auto param = options::get_milliseconds(options, sym_dns_srv_timeout); param) {
    cluster_options.dns().timeout(param.value());
  }
  static const auto sym_dns_srv_nameserver = rb_id2sym(rb_intern("dns_srv_nameserver"));
  if (auto param = options::get_string(options, sym_dns_srv_nameserver); param) {
    static const auto sym_dns_srv_port = rb_id2sym(rb_intern("dns_srv_port"));
    if (auto port = options::get_uint16_t(options, sym_dns_srv_port); port) {
      cluster_options.dns().nameserver(param.value(), port.value());
    } else {
      cluster_options.dns().nameserver(param.value());
    }
  }

  static const auto sym_trust_certificate = rb_id2sym(rb_intern("trust_certificate"));
  if (auto param = options::get_string(options, sym_trust_certificate); param) {
    cluster_options.security().trust_certificate(param.value());
  }

  static const auto sym_trust_certificate_value = rb_id2sym(rb_intern("trust_certificate_value"));
  if (auto param = options::get_string(options, sym_trust_certificate_value); param) {
    cluster_options.security().trust_certificate_value(param.value());
  }

  static const auto sym_tls_verify = rb_id2sym(rb_intern("tls_verify"));
  if (auto mode = options::get_symbol(options, sym_tls_verify); mode) {
    static const auto sym_none = rb_id2sym(rb_intern("none"));
    static const auto sym_peer = rb_id2sym(rb_intern("peer"));
    if (*mode == sym_none) {
      cluster_options.security().tls_verify(tls_verify_mode::none);
    } else if (*mode == sym_peer) {
      cluster_options.security().tls_verify(tls_verify_mode::peer);
    } else {
      throw ruby_exception(
        exc_invalid_argument(),
        rb_sprintf("Failed to select verification mode for TLS: %+" PRIsVALUE, mode.value()));
    }
  }

  static const auto sym_network = rb_id2sym(rb_intern("network"));
  if (auto param = options::get_string(options, sym_network); param) {
    cluster_options.network().preferred_network(param.value());
  }

  static const auto server_group = rb_id2sym(rb_intern("preferred_server_group"));
  if (auto group = options::get_string(options, server_group); group) {
    cluster_options.network().preferred_server_group(group.value());
  }

  static const auto sym_use_ip_protocol = rb_id2sym(rb_intern("use_ip_protocol"));
  if (auto proto = options::get_symbol(options, sym_use_ip_protocol); proto) {
    static const auto sym_any = rb_id2sym(rb_intern("any"));
    static const auto sym_force_ipv4 = rb_id2sym(rb_intern("force_ipv4"));
    static const auto sym_force_ipv6 = rb_id2sym(rb_intern("force_ipv6"));
    if (*proto == sym_any) {
      cluster_options.network().force_ip_protocol(ip_protocol::any);
    } else if (*proto == sym_force_ipv4) {
      cluster_options.network().force_ip_protocol(ip_protocol::force_ipv4);
    } else if (*proto == sym_force_ipv6) {
      cluster_options.network().force_ip_protocol(ip_protocol::force_ipv6);
    } else {
      throw ruby_exception(
        exc_invalid_argument(),
        rb_sprintf("Failed to setect preferred IP protocol: %+" PRIsVALUE, proto.value()));
    }
  }

  static const auto sym_enable_mutation_tokens = rb_id2sym(rb_intern("enable_mutation_tokens"));
  if (auto param = options::get_bool(options, sym_enable_mutation_tokens); param) {
    cluster_options.behavior().enable_mutation_tokens(param.value());
  }

  static const auto sym_show_queries = rb_id2sym(rb_intern("show_queries"));
  if (auto param = options::get_bool(options, sym_show_queries); param) {
    cluster_options.behavior().show_queries(param.value());
  }

  static const auto sym_enable_tcp_keep_alive = rb_id2sym(rb_intern("enable_tcp_keep_alive"));
  if (auto param = options::get_bool(options, sym_enable_tcp_keep_alive); param) {
    cluster_options.network().enable_tcp_keep_alive(param.value());
  }

  static const auto sym_enable_unordered_execution =
    rb_id2sym(rb_intern("enable_unordered_execution"));
  if (auto param = options::get_bool(options, sym_enable_unordered_execution); param) {
    cluster_options.behavior().enable_unordered_execution(param.value());
  }

  static const auto sym_enable_compression = rb_id2sym(rb_intern("enable_compression"));
  if (auto param = options::get_bool(options, sym_enable_compression); param) {
    cluster_options.compression().enabled(param.value());
  }

  static const auto sym_enable_clustermap_notification =
    rb_id2sym(rb_intern("enable_clustermap_notification"));
  if (auto param = options::get_bool(options, sym_enable_clustermap_notification); param) {
    cluster_options.behavior().enable_clustermap_notification(param.value());
  }

  static const auto sym_bootstrap_timeout = rb_id2sym(rb_intern("bootstrap_timeout"));
  if (auto param = options::get_milliseconds(options, sym_bootstrap_timeout); param) {
    cluster_options.timeouts().bootstrap_timeout(param.value());
  }

  static const auto sym_resolve_timeout = rb_id2sym(rb_intern("resolve_timeout"));
  if (auto param = options::get_milliseconds(options, sym_resolve_timeout); param) {
    cluster_options.timeouts().resolve_timeout(param.value());
  }

  static const auto sym_connect_timeout = rb_id2sym(rb_intern("connect_timeout"));
  if (auto param = options::get_milliseconds(options, sym_connect_timeout); param) {
    cluster_options.timeouts().connect_timeout(param.value());
  }

  static const auto sym_key_value_timeout = rb_id2sym(rb_intern("key_value_timeout"));
  if (auto param = options::get_milliseconds(options, sym_key_value_timeout); param) {
    cluster_options.timeouts().key_value_timeout(param.value());
  }

  static const auto sym_key_value_durable_timeout =
    rb_id2sym(rb_intern("key_value_durable_timeout"));
  if (auto param = options::get_milliseconds(options, sym_key_value_durable_timeout); param) {
    cluster_options.timeouts().key_value_durable_timeout(param.value());
  }

  static const auto sym_view_timeout = rb_id2sym(rb_intern("view_timeout"));
  if (auto param = options::get_milliseconds(options, sym_view_timeout); param) {
    cluster_options.timeouts().view_timeout(param.value());
  }

  static const auto sym_query_timeout = rb_id2sym(rb_intern("query_timeout"));
  if (auto param = options::get_milliseconds(options, sym_query_timeout); param) {
    cluster_options.timeouts().query_timeout(param.value());
  }

  static const auto sym_analytics_timeout = rb_id2sym(rb_intern("analytics_timeout"));
  if (auto param = options::get_milliseconds(options, sym_analytics_timeout); param) {
    cluster_options.timeouts().analytics_timeout(param.value());
  }

  static const auto sym_search_timeout = rb_id2sym(rb_intern("search_timeout"));
  if (auto param = options::get_milliseconds(options, sym_search_timeout); param) {
    cluster_options.timeouts().search_timeout(param.value());
  }

  static const auto sym_management_timeout = rb_id2sym(rb_intern("management_timeout"));
  if (auto param = options::get_milliseconds(options, sym_management_timeout); param) {
    cluster_options.timeouts().management_timeout(param.value());
  }

  static const auto sym_tcp_keep_alive_interval = rb_id2sym(rb_intern("tcp_keep_alive_interval"));
  if (auto param = options::get_milliseconds(options, sym_tcp_keep_alive_interval); param) {
    cluster_options.network().tcp_keep_alive_interval(param.value());
  }

  static const auto sym_config_poll_interval = rb_id2sym(rb_intern("config_poll_interval"));
  if (auto param = options::get_milliseconds(options, sym_config_poll_interval); param) {
    cluster_options.network().config_poll_interval(param.value());
  }

  static const auto sym_idle_http_connection_timeout =
    rb_id2sym(rb_intern("idle_http_connection_timeout"));
  if (auto param = options::get_milliseconds(options, sym_idle_http_connection_timeout); param) {
    cluster_options.network().idle_http_connection_timeout(param.value());
  }

  static const auto sym_max_http_connections = rb_id2sym(rb_intern("max_http_connections"));
  if (auto param = options::get_size_t(options, sym_max_http_connections); param) {
    cluster_options.network().max_http_connections(param.value());
  }

  static const auto sym_enable_tracing = rb_id2sym(rb_intern("enable_tracing"));
  if (auto param = options::get_bool(options, sym_enable_tracing); param) {
    cluster_options.tracing().enable(param.value());
  }
  static const auto sym_orphaned_emit_interval = rb_id2sym(rb_intern("orphaned_emit_interval"));
  if (auto param = options::get_milliseconds(options, sym_orphaned_emit_interval); param) {
    cluster_options.tracing().orphaned_emit_interval(param.value());
  }

  static const auto sym_orphaned_sample_size = rb_id2sym(rb_intern("orphaned_sample_size"));
  if (auto param = options::get_size_t(options, sym_orphaned_sample_size); param) {
    cluster_options.tracing().orphaned_sample_size(param.value());
  }

  static const auto sym_threshold_emit_interval = rb_id2sym(rb_intern("threshold_emit_interval"));
  if (auto param = options::get_milliseconds(options, sym_threshold_emit_interval); param) {
    cluster_options.tracing().threshold_emit_interval(param.value());
  }

  static const auto sym_threshold_sample_size = rb_id2sym(rb_intern("threshold_sample_size"));
  if (auto param = options::get_size_t(options, sym_threshold_sample_size); param) {
    cluster_options.tracing().threshold_sample_size(param.value());
  }

  static const auto sym_key_value_threshold = rb_id2sym(rb_intern("key_value_threshold"));
  if (auto param = options::get_milliseconds(options, sym_key_value_threshold); param) {
    cluster_options.tracing().key_value_threshold(param.value());
  }

  static const auto sym_query_threshold = rb_id2sym(rb_intern("query_threshold"));
  if (auto param = options::get_milliseconds(options, sym_query_threshold); param) {
    cluster_options.tracing().query_threshold(param.value());
  }

  static const auto sym_view_threshold = rb_id2sym(rb_intern("view_threshold"));
  if (auto param = options::get_milliseconds(options, sym_view_threshold); param) {
    cluster_options.tracing().view_threshold(param.value());
  }

  static const auto sym_search_threshold = rb_id2sym(rb_intern("search_threshold"));
  if (auto param = options::get_milliseconds(options, sym_search_threshold); param) {
    cluster_options.tracing().search_threshold(param.value());
  }

  static const auto sym_analytics_threshold = rb_id2sym(rb_intern("analytics_threshold"));
  if (auto param = options::get_milliseconds(options, sym_analytics_threshold); param) {
    cluster_options.tracing().analytics_threshold(param.value());
  }

  static const auto sym_management_threshold = rb_id2sym(rb_intern("management_threshold"));
  if (auto param = options::get_milliseconds(options, sym_management_threshold); param) {
    cluster_options.tracing().management_threshold(param.value());
  }

  static const auto sym_enable_metrics = rb_id2sym(rb_intern("enable_metrics"));
  if (auto param = options::get_bool(options, sym_enable_metrics); param) {
    cluster_options.metrics().enable(param.value());
  }

  static const auto sym_metrics_emit_interval = rb_id2sym(rb_intern("metrics_emit_interval"));
  if (auto param = options::get_milliseconds(options, sym_metrics_emit_interval); param) {
    cluster_options.metrics().emit_interval(param.value());
  }

  static const auto sym_app_telemetry = rb_id2sym(rb_intern("application_telemetry"));
  if (auto app_telemetry_options = options::get_hash(options, sym_app_telemetry);
      app_telemetry_options) {
    static const auto sym_enable_app_telemetry = rb_id2sym(rb_intern("enable"));
    if (auto param = options::get_bool(app_telemetry_options.value(), sym_enable_app_telemetry);
        param) {
      cluster_options.application_telemetry().enable(param.value());
    }

    static const auto sym_app_telemetry_endpoint = rb_id2sym(rb_intern("override_endpoint"));
    if (auto param = options::get_string(app_telemetry_options.value(), sym_app_telemetry_endpoint);
        param) {
      cluster_options.application_telemetry().override_endpoint(param.value());
    }

    static const auto sym_app_telemetry_backoff = rb_id2sym(rb_intern("backoff"));
    if (auto param =
          options::get_milliseconds(app_telemetry_options.value(), sym_app_telemetry_backoff);
        param) {
      cluster_options.application_telemetry().backoff_interval(param.value());
    }

    static const auto sym_app_telemetry_ping_interval = rb_id2sym(rb_intern("ping_interval"));
    if (auto param =
          options::get_milliseconds(app_telemetry_options.value(), sym_app_telemetry_ping_interval);
        param) {
      cluster_options.application_telemetry().ping_interval(param.value());
    }

    static const auto sym_app_telemetry_ping_timeout = rb_id2sym(rb_intern("ping_timeout"));
    if (auto param =
          options::get_milliseconds(app_telemetry_options.value(), sym_app_telemetry_ping_timeout);
        param) {
      cluster_options.application_telemetry().ping_timeout(param.value());
    }
  }

  return cluster_options;
}

VALUE
cb_Backend_open(VALUE self, VALUE connstr, VALUE credentials, VALUE options)
{
  cb_backend_data* backend = nullptr;
  TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

  if (backend->instance != nullptr) {
    CB_LOG_TRACE("Trying to open the same backend twice: {}, instance={}",
                 rb_sprintf("%+" PRIsVALUE ", connection_string=%+" PRIsVALUE, self, connstr),
                 static_cast<const void*>(backend->instance.get()));
    return Qnil;
  }

  Check_Type(connstr, T_STRING);

  try {
    auto connection_string = cb_string_new(connstr);
    auto parsed_connection_string = core::utils::parse_connection_string(connection_string);
    if (parsed_connection_string.error) {
      throw ruby_exception(exc_invalid_argument(),
                           fmt::format(R"(Failed to parse connection string "{}": {})",
                                       connection_string,
                                       parsed_connection_string.error.value()));
    }

    auto cluster_options =
      initialize_cluster_options(parsed_connection_string, credentials, options);

    auto promise =
      std::make_shared<std::promise<std::pair<couchbase::error, couchbase::cluster>>>();
    auto f = promise->get_future();
    couchbase::cluster::connect(
      connection_string, cluster_options, [promise](auto&& error, auto&& cluster) {
        promise->set_value({
          std::forward<decltype(error)>(error),
          std::forward<decltype(cluster)>(cluster),
        });
      });
    auto [error, cluster] = cb_wait_for_future(f);
    if (error) {
      cb_throw_error(
        error, fmt::format("failed to connect to the Couchbase Server \"{}\"", connection_string));
    }
    backend->instance = std::make_unique<couchbase::cluster>(std::move(cluster));
    instances.add(backend->instance.get());
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
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
  const auto& cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  bool wait = RTEST(wait_until_ready);

  try {
    std::string name(RSTRING_PTR(bucket), static_cast<std::size_t>(RSTRING_LEN(bucket)));

    if (wait) {
      auto promise = std::make_shared<std::promise<std::error_code>>();
      auto f = promise->get_future();
      cluster.open_bucket(name, [promise](std::error_code ec) {
        promise->set_value(ec);
      });
      if (auto ec = cb_wait_for_future(f)) {
        cb_throw_error_code(ec, fmt::format("unable open bucket \"{}\"", name));
      }
    } else {
      cluster.open_bucket(name, [name](std::error_code ec) {
        CB_LOG_WARNING("unable open bucket \"{}\": {}", name, ec.message());
      });
    }
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_update_credentials(VALUE self, VALUE credentials)
{
  auto cluster = cb_backend_to_public_api_cluster(self);

  try {
    std::variant<couchbase::password_authenticator, couchbase::certificate_authenticator>
      authenticator = construct_authenticator(credentials);

    couchbase::error err{};
    if (std::holds_alternative<couchbase::password_authenticator>(authenticator)) {
      err = cluster.set_authenticator(
        std::get<couchbase::password_authenticator>(std::move(authenticator)));
    } else {
      err = cluster.set_authenticator(
        std::get<couchbase::certificate_authenticator>(std::move(authenticator)));
    }
    if (err) {
      cb_throw_error(err, "failed to update authenticator");
    }
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to update authenticator {}: {}", __func__, se.what()), false));
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
  rb_define_method(cBackend, "update_credentials", cb_Backend_update_credentials, 1);

  rb_define_singleton_method(cBackend, "notify_fork", cb_Backend_notify_fork, 1);
  return cBackend;
}

auto
cb_backend_to_public_api_cluster(VALUE self) -> couchbase::cluster
{
  const cb_backend_data* backend = nullptr;
  TypedData_Get_Struct(self, cb_backend_data, &cb_backend_type, backend);

  if (backend->instance == nullptr) {
    rb_raise(exc_cluster_closed(), "Cluster has been closed already");
  }

  return *backend->instance;
}

auto
cb_backend_to_core_api_cluster(VALUE self) -> core::cluster
{
  return core::get_core_cluster(cb_backend_to_public_api_cluster(self));
}

} // namespace couchbase::ruby
