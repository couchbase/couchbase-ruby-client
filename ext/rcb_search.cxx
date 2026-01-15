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
#include <core/error_context/search.hxx>
#include <core/operations/document_search.hxx>
#include <core/operations/management/search_get_stats.hxx>
#include <core/operations/management/search_index_analyze_document.hxx>
#include <core/operations/management/search_index_control_ingest.hxx>
#include <core/operations/management/search_index_control_plan_freeze.hxx>
#include <core/operations/management/search_index_control_query.hxx>
#include <core/operations/management/search_index_drop.hxx>
#include <core/operations/management/search_index_get.hxx>
#include <core/operations/management/search_index_get_all.hxx>
#include <core/operations/management/search_index_get_documents_count.hxx>
#include <core/operations/management/search_index_get_stats.hxx>
#include <core/operations/management/search_index_upsert.hxx>

#include <gsl/narrow>
#include <spdlog/fmt/bundled/core.h>

#include <future>
#include <memory>

#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_observability.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{
void
cb_extract_search_index(VALUE index, const core::management::search::index& idx)
{
  rb_hash_aset(index, rb_id2sym(rb_intern("uuid")), cb_str_new(idx.uuid));
  rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
  rb_hash_aset(index, rb_id2sym(rb_intern("type")), cb_str_new(idx.type));
  if (!idx.params_json.empty()) {
    rb_hash_aset(index, rb_id2sym(rb_intern("params")), cb_str_new(idx.params_json));
  }

  if (!idx.source_uuid.empty()) {
    rb_hash_aset(index, rb_id2sym(rb_intern("source_uuid")), cb_str_new(idx.source_uuid));
  }
  if (!idx.source_name.empty()) {
    rb_hash_aset(index, rb_id2sym(rb_intern("source_name")), cb_str_new(idx.source_name));
  }
  rb_hash_aset(index, rb_id2sym(rb_intern("source_type")), cb_str_new(idx.source_type));
  if (!idx.source_params_json.empty()) {
    rb_hash_aset(index, rb_id2sym(rb_intern("source_params")), cb_str_new(idx.source_params_json));
  }
  if (!idx.plan_params_json.empty()) {
    rb_hash_aset(index, rb_id2sym(rb_intern("plan_params")), cb_str_new(idx.plan_params_json));
  }
}

VALUE
cb_Backend_search_index_get_all(VALUE self,
                                VALUE bucket,
                                VALUE scope,
                                VALUE options,
                                VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  try {
    core::operations::management::search_index_get_all_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }

    cb_extract_timeout(req, options);
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_get_all_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, "unable to get list of the search indexes");
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    rb_hash_aset(res, rb_id2sym(rb_intern("impl_version")), cb_str_new(resp.impl_version));
    VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
    for (const auto& idx : resp.indexes) {
      VALUE index = rb_hash_new();
      cb_extract_search_index(index, idx);
      rb_ary_push(indexes, index);
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("indexes")), indexes);
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_get(VALUE self,
                            VALUE bucket,
                            VALUE scope,
                            VALUE index_name,
                            VALUE timeout,
                            VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_get_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_get_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(resp.ctx, fmt::format("unable to get search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to get search index \"{}\": {}", req.index_name, resp.error));
      }
    }
    VALUE res = rb_hash_new();
    cb_extract_search_index(res, resp.index);
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_upsert(VALUE self,
                               VALUE bucket,
                               VALUE scope,
                               VALUE index_definition,
                               VALUE timeout,
                               VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_definition, T_HASH);

  try {
    core::operations::management::search_index_upsert_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);

    VALUE index_name = rb_hash_aref(index_definition, rb_id2sym(rb_intern("name")));
    cb_check_type(index_name, T_STRING);
    req.index.name = cb_string_new(index_name);

    VALUE index_type = rb_hash_aref(index_definition, rb_id2sym(rb_intern("type")));
    cb_check_type(index_type, T_STRING);
    req.index.type = cb_string_new(index_type);

    if (VALUE index_uuid = rb_hash_aref(index_definition, rb_id2sym(rb_intern("uuid")));
        !NIL_P(index_uuid)) {
      cb_check_type(index_uuid, T_STRING);
      req.index.uuid = cb_string_new(index_uuid);
    }

    if (VALUE index_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("params")));
        !NIL_P(index_params)) {
      cb_check_type(index_params, T_STRING);
      req.index.params_json = cb_string_new(index_params);
    }

    if (VALUE source_name = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_name")));
        !NIL_P(source_name)) {
      cb_check_type(source_name, T_STRING);
      req.index.source_name = cb_string_new(source_name);
    }

    VALUE source_type = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_type")));
    cb_check_type(source_type, T_STRING);
    req.index.source_type = cb_string_new(source_type);

    if (VALUE source_uuid = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_uuid")));
        !NIL_P(source_uuid)) {
      cb_check_type(source_uuid, T_STRING);
      req.index.source_uuid = cb_string_new(source_uuid);
    }

    if (VALUE source_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("source_params")));
        !NIL_P(source_params)) {
      cb_check_type(source_params, T_STRING);
      req.index.source_params_json = cb_string_new(source_params);
    }

    if (VALUE plan_params = rb_hash_aref(index_definition, rb_id2sym(rb_intern("plan_params")));
        !NIL_P(plan_params)) {
      cb_check_type(plan_params, T_STRING);
      req.index.plan_params_json = cb_string_new(plan_params);
    }

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::search_index_upsert_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to upsert the search index \"{}\"", req.index.name));
      } else {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to upsert the search index \"{}\": {}", req.index.name, resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_drop(VALUE self,
                             VALUE bucket,
                             VALUE scope,
                             VALUE index_name,
                             VALUE timeout,
                             VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_drop_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_drop_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to drop the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to drop the search index \"{}\": {}", req.index_name, resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_get_documents_count(VALUE self,
                                            VALUE bucket,
                                            VALUE scope,
                                            VALUE index_name,
                                            VALUE timeout,
                                            VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_get_documents_count_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_get_documents_count_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to get number of the indexed documents for the search index \"{}\"",
                      req.index_name));
      } else {
        cb_throw_error(
          resp.ctx,
          fmt::format(
            "unable to get number of the indexed documents for the search index \"{}\": {}",
            req.index_name,
            resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    rb_hash_aset(res, rb_id2sym(rb_intern("count")), ULL2NUM(resp.count));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_get_stats(VALUE self,
                                  VALUE index_name,
                                  VALUE timeout,
                                  VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_get_stats_request req{};
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_get_stats_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(
          resp.ctx, fmt::format("unable to get stats for the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to get stats for the search index \"{}\": {}",
                                   req.index_name,
                                   resp.error));
      }
    }
    return cb_str_new(resp.stats);
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_get_stats(VALUE self, VALUE timeout, VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  try {
    core::operations::management::search_get_stats_request req{};
    cb_extract_timeout(req, timeout);
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_get_stats_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, "unable to get stats for the search service");
    }
    return cb_str_new(resp.stats);
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_pause_ingest(VALUE self,
                                     VALUE bucket,
                                     VALUE scope,
                                     VALUE index_name,
                                     VALUE timeout,
                                     VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_control_ingest_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    req.pause = true;
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_control_ingest_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to pause ingest for the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to pause ingest for the search index \"{}\": {}",
                                   req.index_name,
                                   resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_resume_ingest(VALUE self,
                                      VALUE bucket,
                                      VALUE scope,
                                      VALUE index_name,
                                      VALUE timeout,
                                      VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_control_ingest_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    req.pause = false;
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_control_ingest_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to resume ingest for the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to resume ingest for the search index \"{}\": {}",
                                   req.index_name,
                                   resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_allow_querying(VALUE self,
                                       VALUE bucket,
                                       VALUE scope,
                                       VALUE index_name,
                                       VALUE timeout,
                                       VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_control_query_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    req.allow = true;
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_control_query_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to allow querying for the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to allow querying for the search index \"{}\": {}",
                                   req.index_name,
                                   resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_disallow_querying(VALUE self,
                                          VALUE bucket,
                                          VALUE scope,
                                          VALUE index_name,
                                          VALUE timeout,
                                          VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_control_query_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    req.allow = false;
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_control_query_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to disallow querying for the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to disallow querying for the search index \"{}\": {}",
                                   req.index_name,
                                   resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_freeze_plan(VALUE self,
                                    VALUE bucket,
                                    VALUE scope,
                                    VALUE index_name,
                                    VALUE timeout,
                                    VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_control_plan_freeze_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    req.freeze = true;
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_control_plan_freeze_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to freeze for the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to freeze for the search index \"{}\": {}",
                                   req.index_name,
                                   resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_unfreeze_plan(VALUE self,
                                      VALUE bucket,
                                      VALUE scope,
                                      VALUE index_name,
                                      VALUE timeout,
                                      VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);

  try {
    core::operations::management::search_index_control_plan_freeze_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);
    req.index_name = cb_string_new(index_name);
    req.freeze = false;
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::search_index_control_plan_freeze_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to unfreeze plan for the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to unfreeze for the search index \"{}\": {}",
                                   req.index_name,
                                   resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_search_index_analyze_document(VALUE self,
                                         VALUE bucket,
                                         VALUE scope,
                                         VALUE index_name,
                                         VALUE encoded_document,
                                         VALUE timeout,
                                         VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);
  Check_Type(encoded_document, T_STRING);

  try {
    core::operations::management::search_index_analyze_document_request req{};
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    cb_extract_timeout(req, timeout);

    req.index_name = cb_string_new(index_name);
    req.encoded_document = cb_string_new(encoded_document);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::search_index_analyze_document_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.error.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to analyze document using the search index \"{}\"", req.index_name));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to analyze document using the search index \"{}\": {}",
                                   req.index_name,
                                   resp.error));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    rb_hash_aset(res, rb_id2sym(rb_intern("analysis")), cb_str_new(resp.analysis));
    return res;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

int
cb_for_each_raw_param(VALUE key, VALUE value, VALUE arg)
{
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  auto* preq = reinterpret_cast<core::operations::search_request*>(arg);
  try {
    cb_check_type(key, T_STRING);
    cb_check_type(value, T_STRING);
  } catch (const ruby_exception&) {
    return ST_STOP;
  }
  preq->raw[cb_string_new(key)] = cb_string_new(value);
  return ST_CONTINUE;
}

VALUE
cb_Backend_document_search(VALUE self,
                           VALUE bucket,
                           VALUE scope,
                           VALUE index_name,
                           VALUE query,
                           VALUE search_request,
                           VALUE options,
                           VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(index_name, T_STRING);
  Check_Type(query, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::search_request req;
    if (!NIL_P(bucket)) {
      cb_check_type(bucket, T_STRING);
      req.bucket_name = cb_string_new(bucket);
    }
    if (!NIL_P(scope)) {
      cb_check_type(scope, T_STRING);
      req.scope_name = cb_string_new(scope);
    }
    if (VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id")));
        !NIL_P(client_context_id)) {
      cb_check_type(client_context_id, T_STRING);
      req.client_context_id = cb_string_new(client_context_id);
    }
    cb_extract_timeout(req, options);
    req.index_name = cb_string_new(index_name);
    req.query = cb_string_new(query);

    cb_extract_option_bool(req.explain, options, "explain");
    cb_extract_option_bool(req.disable_scoring, options, "disable_scoring");
    cb_extract_option_bool(req.include_locations, options, "include_locations");
    cb_extract_option_bool(req.show_request, options, "show_request");

    if (VALUE vector_options = rb_hash_aref(search_request, rb_id2sym(rb_intern("vector_search")));
        !NIL_P(vector_options)) {
      cb_check_type(vector_options, T_HASH);
      if (VALUE vector_queries =
            rb_hash_aref(vector_options, rb_id2sym(rb_intern("vector_queries")));
          !NIL_P(vector_queries)) {
        cb_check_type(vector_queries, T_STRING);
        req.vector_search = cb_string_new(vector_queries);
      }
      if (VALUE vector_query_combination =
            rb_hash_aref(vector_options, rb_id2sym(rb_intern("vector_query_combination")));
          !NIL_P(vector_query_combination)) {
        cb_check_type(vector_query_combination, T_SYMBOL);
        ID type = rb_sym2id(vector_query_combination);
        if (type == rb_intern("and")) {
          req.vector_query_combination = core::vector_query_combination::combination_and;
        } else if (type == rb_intern("or")) {
          req.vector_query_combination = core::vector_query_combination::combination_or;
        }
      }
    }

    if (VALUE skip = rb_hash_aref(options, rb_id2sym(rb_intern("skip"))); !NIL_P(skip)) {
      cb_check_type(skip, T_FIXNUM);
      req.skip = FIX2ULONG(skip);
    }
    if (VALUE limit = rb_hash_aref(options, rb_id2sym(rb_intern("limit"))); !NIL_P(limit)) {
      cb_check_type(limit, T_FIXNUM);
      req.limit = FIX2ULONG(limit);
    }
    if (VALUE highlight_style = rb_hash_aref(options, rb_id2sym(rb_intern("highlight_style")));
        !NIL_P(highlight_style)) {
      cb_check_type(highlight_style, T_SYMBOL);
      ID type = rb_sym2id(highlight_style);
      if (type == rb_intern("html")) {
        req.highlight_style = core::search_highlight_style::html;
      } else if (type == rb_intern("ansi")) {
        req.highlight_style = core::search_highlight_style::ansi;
      }
    }

    if (VALUE highlight_fields = rb_hash_aref(options, rb_id2sym(rb_intern("highlight_fields")));
        !NIL_P(highlight_fields)) {
      cb_check_type(highlight_fields, T_ARRAY);
      auto highlight_fields_size = static_cast<std::size_t>(RARRAY_LEN(highlight_fields));
      req.highlight_fields.reserve(highlight_fields_size);
      for (std::size_t i = 0; i < highlight_fields_size; ++i) {
        VALUE field = rb_ary_entry(highlight_fields, static_cast<long>(i));
        cb_check_type(field, T_STRING);
        req.highlight_fields.emplace_back(cb_string_new(field));
      }
    }

    if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency")));
        !NIL_P(scan_consistency)) {
      cb_check_type(scan_consistency, T_SYMBOL);
      if (ID type = rb_sym2id(scan_consistency); type == rb_intern("not_bounded")) {
        req.scan_consistency = core::search_scan_consistency::not_bounded;
      }
    }

    if (VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state")));
        !NIL_P(mutation_state)) {
      cb_check_type(mutation_state, T_ARRAY);
      auto state_size = static_cast<std::size_t>(RARRAY_LEN(mutation_state));
      req.mutation_state.reserve(state_size);
      for (std::size_t i = 0; i < state_size; ++i) {
        VALUE token = rb_ary_entry(mutation_state, static_cast<long>(i));
        cb_check_type(token, T_HASH);
        VALUE bucket_name = rb_hash_aref(token, rb_id2sym(rb_intern("bucket_name")));
        cb_check_type(bucket_name, T_STRING);
        VALUE partition_id = rb_hash_aref(token, rb_id2sym(rb_intern("partition_id")));
        cb_check_type(partition_id, T_FIXNUM);
        VALUE partition_uuid = rb_hash_aref(token, rb_id2sym(rb_intern("partition_uuid")));
        switch (TYPE(partition_uuid)) {
          case T_FIXNUM:
          case T_BIGNUM:
            break;
          default:
            throw ruby_exception(rb_eArgError, "partition_uuid must be an Integer");
        }
        VALUE sequence_number = rb_hash_aref(token, rb_id2sym(rb_intern("sequence_number")));
        switch (TYPE(sequence_number)) {
          case T_FIXNUM:
          case T_BIGNUM:
            break;
          default:
            throw ruby_exception(rb_eArgError, "sequence_number must be an Integer");
        }
        req.mutation_state.emplace_back(NUM2ULL(partition_uuid),
                                        NUM2ULL(sequence_number),
                                        gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                        cb_string_new(bucket_name));
      }
    }

    if (VALUE fields = rb_hash_aref(options, rb_id2sym(rb_intern("fields"))); !NIL_P(fields)) {
      cb_check_type(fields, T_ARRAY);
      auto fields_size = static_cast<std::size_t>(RARRAY_LEN(fields));
      req.fields.reserve(fields_size);
      for (std::size_t i = 0; i < fields_size; ++i) {
        VALUE field = rb_ary_entry(fields, static_cast<long>(i));
        cb_check_type(field, T_STRING);
        req.fields.emplace_back(cb_string_new(field));
      }
    }

    VALUE collections = rb_hash_aref(options, rb_id2sym(rb_intern("collections")));
    if (!NIL_P(collections)) {
      cb_check_type(collections, T_ARRAY);
      auto collections_size = static_cast<std::size_t>(RARRAY_LEN(collections));
      req.collections.reserve(collections_size);
      for (std::size_t i = 0; i < collections_size; ++i) {
        VALUE collection = rb_ary_entry(collections, static_cast<long>(i));
        cb_check_type(collection, T_STRING);
        req.collections.emplace_back(cb_string_new(collection));
      }
    }

    if (VALUE sort = rb_hash_aref(options, rb_id2sym(rb_intern("sort"))); !NIL_P(sort)) {
      cb_check_type(sort, T_ARRAY);
      for (std::size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(sort)); ++i) {
        VALUE sort_spec = rb_ary_entry(sort, static_cast<long>(i));
        req.sort_specs.emplace_back(cb_string_new(sort_spec));
      }
    }

    if (VALUE facets = rb_hash_aref(options, rb_id2sym(rb_intern("facets"))); !NIL_P(facets)) {
      cb_check_type(facets, T_ARRAY);
      for (std::size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(facets)); ++i) {
        VALUE facet_pair = rb_ary_entry(facets, static_cast<long>(i));
        cb_check_type(facet_pair, T_ARRAY);
        if (RARRAY_LEN(facet_pair) == 2) {
          VALUE facet_name = rb_ary_entry(facet_pair, 0);
          cb_check_type(facet_name, T_STRING);
          VALUE facet_definition = rb_ary_entry(facet_pair, 1);
          cb_check_type(facet_definition, T_STRING);
          req.facets.try_emplace(cb_string_new(facet_name), cb_string_new(facet_definition));
        }
      }
    }

    if (VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters")));
        !NIL_P(raw_params)) {
      cb_check_type(raw_params, T_HASH);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      rb_hash_foreach(raw_params, cb_for_each_raw_param, reinterpret_cast<VALUE>(&req));
    }

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::search_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format("unable to perform search query for index \"{}\": {}",
                                 req.index_name,
                                 resp.error));
    }
    VALUE res = rb_hash_new();

    VALUE meta_data = rb_hash_new();
    rb_hash_aset(meta_data,
                 rb_id2sym(rb_intern("client_context_id")),
                 cb_str_new(resp.meta.client_context_id));

    VALUE metrics = rb_hash_new();
    rb_hash_aset(
      metrics,
      rb_id2sym(rb_intern("took")),
      LL2NUM(
        std::chrono::duration_cast<std::chrono::milliseconds>(resp.meta.metrics.took).count()));
    rb_hash_aset(
      metrics, rb_id2sym(rb_intern("total_rows")), ULL2NUM(resp.meta.metrics.total_rows));
    rb_hash_aset(metrics, rb_id2sym(rb_intern("max_score")), DBL2NUM(resp.meta.metrics.max_score));
    rb_hash_aset(metrics,
                 rb_id2sym(rb_intern("success_partition_count")),
                 ULL2NUM(resp.meta.metrics.success_partition_count));
    rb_hash_aset(metrics,
                 rb_id2sym(rb_intern("error_partition_count")),
                 ULL2NUM(resp.meta.metrics.error_partition_count));
    rb_hash_aset(meta_data, rb_id2sym(rb_intern("metrics")), metrics);

    if (!resp.meta.errors.empty()) {
      VALUE errors = rb_hash_new();
      for (const auto& [code, message] : resp.meta.errors) {
        rb_hash_aset(errors, cb_str_new(code), cb_str_new(message));
      }
      rb_hash_aset(meta_data, rb_id2sym(rb_intern("errors")), errors);
    }

    rb_hash_aset(res, rb_id2sym(rb_intern("meta_data")), meta_data);

    VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
    for (const auto& entry : resp.rows) {
      VALUE row = rb_hash_new();
      rb_hash_aset(row, rb_id2sym(rb_intern("index")), cb_str_new(entry.index));
      rb_hash_aset(row, rb_id2sym(rb_intern("id")), cb_str_new(entry.id));
      rb_hash_aset(row, rb_id2sym(rb_intern("score")), DBL2NUM(entry.score));
      VALUE locations = rb_ary_new_capa(static_cast<long>(entry.locations.size()));
      for (const auto& loc : entry.locations) {
        VALUE location = rb_hash_new();
        rb_hash_aset(location, rb_id2sym(rb_intern("field")), cb_str_new(loc.field));
        rb_hash_aset(location, rb_id2sym(rb_intern("term")), cb_str_new(loc.term));
        rb_hash_aset(location, rb_id2sym(rb_intern("pos")), ULL2NUM(loc.position));
        rb_hash_aset(location, rb_id2sym(rb_intern("start_offset")), ULL2NUM(loc.start_offset));
        rb_hash_aset(location, rb_id2sym(rb_intern("end_offset")), ULL2NUM(loc.end_offset));
        if (loc.array_positions) {
          VALUE ap = rb_ary_new_capa(static_cast<long>(loc.array_positions->size()));
          for (const auto& pos : *loc.array_positions) {
            rb_ary_push(ap, ULL2NUM(pos));
          }
          rb_hash_aset(location, rb_id2sym(rb_intern("array_positions")), ap);
        }
        rb_ary_push(locations, location);
      }
      rb_hash_aset(row, rb_id2sym(rb_intern("locations")), locations);
      if (!entry.fragments.empty()) {
        VALUE fragments = rb_hash_new();
        for (const auto& [field, field_fragments] : entry.fragments) {
          VALUE fragments_list = rb_ary_new_capa(static_cast<long>(field_fragments.size()));
          for (const auto& fragment : field_fragments) {
            rb_ary_push(fragments_list, cb_str_new(fragment));
          }
          rb_hash_aset(fragments, cb_str_new(field), fragments_list);
        }
        rb_hash_aset(row, rb_id2sym(rb_intern("fragments")), fragments);
      }
      if (!entry.fields.empty()) {
        rb_hash_aset(row, rb_id2sym(rb_intern("fields")), cb_str_new(entry.fields));
      }
      if (!entry.explanation.empty()) {
        rb_hash_aset(row, rb_id2sym(rb_intern("explanation")), cb_str_new(entry.explanation));
      }
      rb_ary_push(rows, row);
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);

    if (!resp.facets.empty()) {
      VALUE result_facets = rb_hash_new();
      for (const auto& entry : resp.facets) {
        VALUE facet = rb_hash_new();
        VALUE facet_name = cb_str_new(entry.name);
        rb_hash_aset(facet, rb_id2sym(rb_intern("name")), facet_name);
        rb_hash_aset(facet, rb_id2sym(rb_intern("field")), cb_str_new(entry.field));
        rb_hash_aset(facet, rb_id2sym(rb_intern("total")), ULL2NUM(entry.total));
        rb_hash_aset(facet, rb_id2sym(rb_intern("missing")), ULL2NUM(entry.missing));
        rb_hash_aset(facet, rb_id2sym(rb_intern("other")), ULL2NUM(entry.other));
        if (!entry.terms.empty()) {
          VALUE terms = rb_ary_new_capa(static_cast<long>(entry.terms.size()));
          for (const auto& item : entry.terms) {
            VALUE term = rb_hash_new();
            rb_hash_aset(term, rb_id2sym(rb_intern("term")), cb_str_new(item.term));
            rb_hash_aset(term, rb_id2sym(rb_intern("count")), ULL2NUM(item.count));
            rb_ary_push(terms, term);
          }
          rb_hash_aset(facet, rb_id2sym(rb_intern("terms")), terms);
        } else if (!entry.date_ranges.empty()) {
          VALUE date_ranges = rb_ary_new_capa(static_cast<long>(entry.date_ranges.size()));
          for (const auto& item : entry.date_ranges) {
            VALUE date_range = rb_hash_new();
            rb_hash_aset(date_range, rb_id2sym(rb_intern("name")), cb_str_new(item.name));
            rb_hash_aset(date_range, rb_id2sym(rb_intern("count")), ULL2NUM(item.count));
            if (item.start) {
              rb_hash_aset(
                date_range, rb_id2sym(rb_intern("start_time")), cb_str_new(item.start.value()));
            }
            if (item.end) {
              rb_hash_aset(
                date_range, rb_id2sym(rb_intern("end_time")), cb_str_new(item.end.value()));
            }
            rb_ary_push(date_ranges, date_range);
          }
          rb_hash_aset(facet, rb_id2sym(rb_intern("date_ranges")), date_ranges);
        } else if (!entry.numeric_ranges.empty()) {
          VALUE numeric_ranges = rb_ary_new_capa(static_cast<long>(entry.numeric_ranges.size()));
          for (const auto& item : entry.numeric_ranges) {
            VALUE numeric_range = rb_hash_new();
            rb_hash_aset(numeric_range, rb_id2sym(rb_intern("name")), cb_str_new(item.name));
            rb_hash_aset(numeric_range, rb_id2sym(rb_intern("count")), ULL2NUM(item.count));
            if (std::holds_alternative<double>(item.min)) {
              rb_hash_aset(
                numeric_range, rb_id2sym(rb_intern("min")), DBL2NUM(std::get<double>(item.min)));
            } else if (std::holds_alternative<std::uint64_t>(item.min)) {
              rb_hash_aset(numeric_range,
                           rb_id2sym(rb_intern("min")),
                           ULL2NUM(std::get<std::uint64_t>(item.min)));
            }
            if (std::holds_alternative<double>(item.max)) {
              rb_hash_aset(
                numeric_range, rb_id2sym(rb_intern("max")), DBL2NUM(std::get<double>(item.max)));
            } else if (std::holds_alternative<std::uint64_t>(item.max)) {
              rb_hash_aset(numeric_range,
                           rb_id2sym(rb_intern("max")),
                           ULL2NUM(std::get<std::uint64_t>(item.max)));
            }
            rb_ary_push(numeric_ranges, numeric_range);
          }
          rb_hash_aset(facet, rb_id2sym(rb_intern("numeric_ranges")), numeric_ranges);
        }
        rb_hash_aset(result_facets, facet_name, facet);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("facets")), result_facets);
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
init_search(VALUE cBackend)
{
  rb_define_method(cBackend, "document_search", cb_Backend_document_search, 7);

  rb_define_method(cBackend, "search_get_stats", cb_Backend_search_get_stats, 2);
  rb_define_method(cBackend, "search_index_get_all", cb_Backend_search_index_get_all, 4);
  rb_define_method(cBackend, "search_index_get", cb_Backend_search_index_get, 5);
  rb_define_method(cBackend, "search_index_upsert", cb_Backend_search_index_upsert, 5);
  rb_define_method(cBackend, "search_index_drop", cb_Backend_search_index_drop, 5);
  rb_define_method(cBackend, "search_index_get_stats", cb_Backend_search_index_get_stats, 3);
  rb_define_method(
    cBackend, "search_index_get_documents_count", cb_Backend_search_index_get_documents_count, 5);
  rb_define_method(cBackend, "search_index_pause_ingest", cb_Backend_search_index_pause_ingest, 5);
  rb_define_method(
    cBackend, "search_index_resume_ingest", cb_Backend_search_index_resume_ingest, 5);
  rb_define_method(
    cBackend, "search_index_allow_querying", cb_Backend_search_index_allow_querying, 5);
  rb_define_method(
    cBackend, "search_index_disallow_querying", cb_Backend_search_index_disallow_querying, 5);
  rb_define_method(cBackend, "search_index_freeze_plan", cb_Backend_search_index_freeze_plan, 5);
  rb_define_method(
    cBackend, "search_index_unfreeze_plan", cb_Backend_search_index_unfreeze_plan, 5);
  rb_define_method(
    cBackend, "search_index_analyze_document", cb_Backend_search_index_analyze_document, 6);
}
} // namespace couchbase::ruby
