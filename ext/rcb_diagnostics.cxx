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

#include <fmt/core.h>

#include <future>

#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{
VALUE
cb_Backend_diagnostics(VALUE self, VALUE report_id)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  if (!NIL_P(report_id)) {
    Check_Type(report_id, T_STRING);
  }

  try {
    std::optional<std::string> id;
    if (!NIL_P(report_id)) {
      id.emplace(cb_string_new(report_id));
    }
    std::promise<core::diag::diagnostics_result> promise;
    auto f = promise.get_future();
    cluster.diagnostics(id, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("id")), cb_str_new(resp.id));
    rb_hash_aset(res, rb_id2sym(rb_intern("sdk")), cb_str_new(resp.sdk));
    rb_hash_aset(res, rb_id2sym(rb_intern("version")), INT2FIX(resp.version));
    VALUE services = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("services")), services);
    for (const auto& [service_type, service_infos] : resp.services) {
      VALUE type = Qnil;
      switch (service_type) {
        case core::service_type::key_value:
          type = rb_id2sym(rb_intern("kv"));
          break;
        case core::service_type::query:
          type = rb_id2sym(rb_intern("query"));
          break;
        case core::service_type::analytics:
          type = rb_id2sym(rb_intern("analytics"));
          break;
        case core::service_type::search:
          type = rb_id2sym(rb_intern("search"));
          break;
        case core::service_type::view:
          type = rb_id2sym(rb_intern("views"));
          break;
        case core::service_type::management:
          type = rb_id2sym(rb_intern("mgmt"));
          break;
        case core::service_type::eventing:
          type = rb_id2sym(rb_intern("eventing"));
          break;
      }
      VALUE endpoints = rb_ary_new();
      rb_hash_aset(services, type, endpoints);
      for (const auto& svc : service_infos) {
        VALUE service = rb_hash_new();
        if (svc.last_activity) {
          rb_hash_aset(
            service, rb_id2sym(rb_intern("last_activity_us")), LL2NUM(svc.last_activity->count()));
        }
        rb_hash_aset(service, rb_id2sym(rb_intern("id")), cb_str_new(svc.id));
        rb_hash_aset(service, rb_id2sym(rb_intern("remote")), cb_str_new(svc.remote));
        rb_hash_aset(service, rb_id2sym(rb_intern("local")), cb_str_new(svc.local));
        VALUE state = Qnil;
        switch (svc.state) {
          case core::diag::endpoint_state::disconnected:
            state = rb_id2sym(rb_intern("disconnected"));
            break;
          case core::diag::endpoint_state::connecting:
            state = rb_id2sym(rb_intern("connecting"));
            break;
          case core::diag::endpoint_state::connected:
            state = rb_id2sym(rb_intern("connected"));
            break;
          case core::diag::endpoint_state::disconnecting:
            state = rb_id2sym(rb_intern("disconnecting"));
            break;
        }
        if (svc.details) {
          rb_hash_aset(service, rb_id2sym(rb_intern("details")), cb_str_new(svc.details.value()));
        }
        rb_hash_aset(service, rb_id2sym(rb_intern("state")), state);
        rb_ary_push(endpoints, service);
      }
    }
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
    return Qnil;
  }
}

VALUE
cb_Backend_ping(VALUE self, VALUE bucket, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  if (!NIL_P(bucket)) {
    Check_Type(bucket, T_STRING);
  }

  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }
  try {
    VALUE id = Qnil;
    cb_extract_option_string(id, options, "report_id");
    std::optional<std::string> report_id{};
    if (!NIL_P(id)) {
      report_id.emplace(cb_string_new(id));
    }
    std::optional<std::string> bucket_name{};
    if (!NIL_P(bucket)) {
      bucket_name.emplace(cb_string_new(bucket));
    }
    VALUE services = Qnil;
    cb_extract_option_array(services, options, "service_types");
    std::set<core::service_type> selected_services{};
    if (!NIL_P(services)) {
      auto entries_num = static_cast<std::size_t>(RARRAY_LEN(services));
      for (std::size_t i = 0; i < entries_num; ++i) {
        VALUE entry = rb_ary_entry(services, static_cast<long>(i));
        if (entry == rb_id2sym(rb_intern("kv"))) {
          selected_services.insert(core::service_type::key_value);
        } else if (entry == rb_id2sym(rb_intern("query"))) {
          selected_services.insert(core::service_type::query);
        } else if (entry == rb_id2sym(rb_intern("analytics"))) {
          selected_services.insert(core::service_type::analytics);
        } else if (entry == rb_id2sym(rb_intern("search"))) {
          selected_services.insert(core::service_type::search);
        } else if (entry == rb_id2sym(rb_intern("views"))) {
          selected_services.insert(core::service_type::view);
        } else if (entry == rb_id2sym(rb_intern("management"))) {
          selected_services.insert(core::service_type::management);
        } else if (entry == rb_id2sym(rb_intern("eventing"))) {
          selected_services.insert(core::service_type::eventing);
        }
      }
    }
    std::optional<std::chrono::milliseconds> timeout{};
    cb_extract_timeout(timeout, options);

    std::promise<core::diag::ping_result> promise;
    auto f = promise.get_future();
    cluster.ping(report_id,
                 bucket_name,
                 selected_services,
                 timeout,
                 [promise = std::move(promise)](auto&& resp) mutable {
                   promise.set_value(std::forward<decltype(resp)>(resp));
                 });
    auto resp = cb_wait_for_future(f);

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("id")), cb_str_new(resp.id));
    rb_hash_aset(res, rb_id2sym(rb_intern("sdk")), cb_str_new(resp.sdk));
    rb_hash_aset(res, rb_id2sym(rb_intern("version")), INT2FIX(resp.version));
    services = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("services")), services);
    for (const auto& [service_type, service_infos] : resp.services) {
      VALUE type = Qnil;
      switch (service_type) {
        case core::service_type::key_value:
          type = rb_id2sym(rb_intern("kv"));
          break;
        case core::service_type::query:
          type = rb_id2sym(rb_intern("query"));
          break;
        case core::service_type::analytics:
          type = rb_id2sym(rb_intern("analytics"));
          break;
        case core::service_type::search:
          type = rb_id2sym(rb_intern("search"));
          break;
        case core::service_type::view:
          type = rb_id2sym(rb_intern("views"));
          break;
        case core::service_type::management:
          type = rb_id2sym(rb_intern("management"));
          break;
        case core::service_type::eventing:
          type = rb_id2sym(rb_intern("eventing"));
          break;
      }
      VALUE endpoints = rb_ary_new();
      rb_hash_aset(services, type, endpoints);
      for (const auto& svc : service_infos) {
        VALUE service = rb_hash_new();
        rb_hash_aset(service, rb_id2sym(rb_intern("latency")), LL2NUM(svc.latency.count()));
        rb_hash_aset(service, rb_id2sym(rb_intern("id")), cb_str_new(svc.id));
        rb_hash_aset(service, rb_id2sym(rb_intern("remote")), cb_str_new(svc.remote));
        rb_hash_aset(service, rb_id2sym(rb_intern("local")), cb_str_new(svc.local));
        VALUE state = Qnil;
        switch (svc.state) {
          case core::diag::ping_state::ok:
            state = rb_id2sym(rb_intern("ok"));
            break;
          case core::diag::ping_state::timeout:
            state = rb_id2sym(rb_intern("timeout"));
            break;
          case core::diag::ping_state::error:
            state = rb_id2sym(rb_intern("error"));
            if (svc.error) {
              rb_hash_aset(service, rb_id2sym(rb_intern("error")), cb_str_new(svc.error.value()));
            }
            break;
        }
        rb_hash_aset(service, rb_id2sym(rb_intern("state")), state);
        rb_ary_push(endpoints, service);
      }
    }
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}
} // namespace

void
init_diagnostics(VALUE cBackend)
{
  rb_define_method(cBackend, "diagnostics", cb_Backend_diagnostics, 1);
  rb_define_method(cBackend, "ping", cb_Backend_ping, 2);
}
} // namespace couchbase::ruby
