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
#include <core/operations/document_query.hxx>
#include <core/operations/management/query_index_build_deferred.hxx>
#include <core/operations/management/query_index_create.hxx>
#include <core/operations/management/query_index_drop.hxx>
#include <core/operations/management/query_index_get_all.hxx>

#include <gsl/narrow>

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
VALUE
cb_Backend_query_index_get_all(VALUE self,
                               VALUE bucket_name,
                               VALUE options,
                               VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_get_all_request req{};
    req.bucket_name = cb_string_new(bucket_name);
    cb_extract_timeout(req, options);
    if (!NIL_P(options)) {
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::query_index_get_all_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      cb_throw_error(
        resp.ctx,
        fmt::format("unable to get list of the indexes of the bucket \"{}\"", req.bucket_name));
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
    for (const auto& idx : resp.indexes) {
      VALUE index = rb_hash_new();
      rb_hash_aset(index, rb_id2sym(rb_intern("state")), rb_id2sym(rb_intern(idx.state.c_str())));
      rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
      rb_hash_aset(index, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern(idx.type.c_str())));
      rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
      VALUE index_key = rb_ary_new_capa(static_cast<long>(idx.index_key.size()));
      for (const auto& key : idx.index_key) {
        rb_ary_push(index_key, cb_str_new(key));
      }
      rb_hash_aset(index, rb_id2sym(rb_intern("index_key")), index_key);
      if (idx.collection_name) {
        rb_hash_aset(
          index, rb_id2sym(rb_intern("collection_name")), cb_str_new(idx.collection_name.value()));
      }
      if (idx.scope_name) {
        rb_hash_aset(index, rb_id2sym(rb_intern("scope_name")), cb_str_new(idx.scope_name.value()));
      }
      rb_hash_aset(index, rb_id2sym(rb_intern("bucket_name")), cb_str_new(idx.bucket_name));
      if (idx.condition) {
        rb_hash_aset(index, rb_id2sym(rb_intern("condition")), cb_str_new(idx.condition.value()));
      }
      if (idx.partition) {
        rb_hash_aset(index, rb_id2sym(rb_intern("partition")), cb_str_new(idx.partition.value()));
      }
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
cb_Backend_query_index_create(VALUE self,
                              VALUE bucket_name,
                              VALUE index_name,
                              VALUE keys,
                              VALUE options,
                              VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(index_name, T_STRING);
  Check_Type(keys, T_ARRAY);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_create_request req{};
    cb_extract_timeout(req, options);
    req.bucket_name = cb_string_new(bucket_name);
    req.index_name = cb_string_new(index_name);
    auto keys_num = static_cast<std::size_t>(RARRAY_LEN(keys));
    req.keys.reserve(keys_num);
    for (std::size_t i = 0; i < keys_num; ++i) {
      VALUE entry = rb_ary_entry(keys, static_cast<long>(i));
      cb_check_type(entry, T_STRING);
      req.keys.emplace_back(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry)));
    }
    if (!NIL_P(options)) {
      if (VALUE ignore_if_exists = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_exists")));
          ignore_if_exists == Qtrue) {
        req.ignore_if_exists = true;
      } else if (ignore_if_exists == Qfalse) {
        req.ignore_if_exists = false;
      } /* else use backend default */
      if (VALUE deferred = rb_hash_aref(options, rb_id2sym(rb_intern("deferred")));
          deferred == Qtrue) {
        req.deferred = true;
      } else if (deferred == Qfalse) {
        req.deferred = false;
      } /* else use backend default */
      if (VALUE num_replicas = rb_hash_aref(options, rb_id2sym(rb_intern("num_replicas")));
          !NIL_P(num_replicas)) {
        req.num_replicas = NUM2UINT(num_replicas);
      } /* else use backend default */
      if (VALUE condition = rb_hash_aref(options, rb_id2sym(rb_intern("condition")));
          !NIL_P(condition)) {
        req.condition.emplace(cb_string_new(condition));
      } /* else use backend default */
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_create_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to create index "{}" on the bucket "{}" ({}: {}))",
                                   req.index_name,
                                   req.bucket_name,
                                   first_error.code,
                                   first_error.message));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to create index "{}" on the bucket "{}")",
                                   req.index_name,
                                   req.bucket_name));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

VALUE
cb_Backend_query_index_drop(VALUE self,
                            VALUE bucket_name,
                            VALUE index_name,
                            VALUE options,
                            VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(index_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_drop_request req{};
    cb_extract_timeout(req, options);
    req.bucket_name = cb_string_new(bucket_name);
    req.index_name = cb_string_new(index_name);
    if (!NIL_P(options)) {
      if (VALUE ignore_if_does_not_exist =
            rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_does_not_exist")));
          ignore_if_does_not_exist == Qtrue) {
        req.ignore_if_does_not_exist = true;
      } else if (ignore_if_does_not_exist == Qfalse) {
        req.ignore_if_does_not_exist = false;
      } /* else use backend default */

      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_drop_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to drop index "{}" on the bucket "{}" ({}: {}))",
                                   req.index_name,
                                   req.bucket_name,
                                   first_error.code,
                                   first_error.message));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to drop index "{}" on the bucket "{}")",
                                   req.index_name,
                                   req.bucket_name));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

VALUE
cb_Backend_query_index_create_primary(VALUE self,
                                      VALUE bucket_name,
                                      VALUE options,
                                      VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_create_request req{};
    cb_extract_timeout(req, options);
    req.is_primary = true;
    req.bucket_name = cb_string_new(bucket_name);
    if (!NIL_P(options)) {
      if (VALUE ignore_if_exists = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_exists")));
          ignore_if_exists == Qtrue) {
        req.ignore_if_exists = true;
      } else if (ignore_if_exists == Qfalse) {
        req.ignore_if_exists = false;
      } /* else use backend default */
      if (VALUE deferred = rb_hash_aref(options, rb_id2sym(rb_intern("deferred")));
          deferred == Qtrue) {
        req.deferred = true;
      } else if (deferred == Qfalse) {
        req.deferred = false;
      } /* else use backend default */
      if (VALUE num_replicas = rb_hash_aref(options, rb_id2sym(rb_intern("num_replicas")));
          !NIL_P(num_replicas)) {
        req.num_replicas = NUM2UINT(num_replicas);
      } /* else use backend default */
      if (VALUE index_name = rb_hash_aref(options, rb_id2sym(rb_intern("index_name")));
          TYPE(index_name) == T_STRING) {
        req.index_name = cb_string_new(index_name);
      } /* else use backend default */
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_create_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to create primary index on the bucket "{}" ({}: {}))",
                                   req.bucket_name,
                                   first_error.code,
                                   first_error.message));
      } else {
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to create primary index on the bucket "{}")", req.bucket_name));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

VALUE
cb_Backend_query_index_drop_primary(VALUE self,
                                    VALUE bucket_name,
                                    VALUE options,
                                    VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_drop_request req{};
    cb_extract_timeout(req, options);
    req.is_primary = true;
    req.bucket_name = cb_string_new(bucket_name);
    if (!NIL_P(options)) {
      if (VALUE ignore_if_does_not_exist =
            rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_does_not_exist")));
          ignore_if_does_not_exist == Qtrue) {
        req.ignore_if_does_not_exist = true;
      } else if (ignore_if_does_not_exist == Qfalse) {
        req.ignore_if_does_not_exist = false;
      } /* else use backend default */
      if (VALUE index_name = rb_hash_aref(options, rb_id2sym(rb_intern("index_name")));
          !NIL_P(index_name)) {
        cb_check_type(options, T_STRING);
        req.is_primary = false;
        req.bucket_name = cb_string_new(index_name);
      }
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_drop_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to drop primary index on the bucket "{}" ({}: {}))",
                                   req.bucket_name,
                                   first_error.code,
                                   first_error.message));
      } else {
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to drop primary index on the bucket "{}")", req.bucket_name));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

VALUE
cb_Backend_query_index_build_deferred(VALUE self,
                                      VALUE bucket_name,
                                      VALUE options,
                                      VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_build_deferred_request req{};
    cb_extract_timeout(req, options);
    req.bucket_name = cb_string_new(bucket_name);

    if (!NIL_P(options)) {
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_build_deferred_response> promise;
    auto f = promise.get_future();

    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to build deferred indexes on the bucket "{}" ({}: {}))",
                      first_error.code,

                      first_error.message));
      } else {
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to build deferred indexes on the bucket "{}")", req.bucket_name));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

int
cb_for_each_named_param(VALUE key, VALUE value, VALUE arg)
{
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  auto* preq = reinterpret_cast<core::operations::query_request*>(arg);
  try {
    cb_check_type(key, T_STRING);
    cb_check_type(value, T_STRING);
  } catch (const ruby_exception&) {
    return ST_STOP;
  }
  preq->named_parameters[cb_string_new(key)] = cb_string_new(value);
  return ST_CONTINUE;
}

int
cb_for_each_raw_param(VALUE key, VALUE value, VALUE arg)
{
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  auto* preq = reinterpret_cast<core::operations::query_request*>(arg);
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
cb_Backend_document_query(VALUE self, VALUE statement, VALUE options, VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(statement, T_STRING);
  Check_Type(options, T_HASH);

  try {
    core::operations::query_request req;
    req.statement = cb_string_new(statement);
    if (VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id")));
        !NIL_P(client_context_id)) {
      cb_check_type(client_context_id, T_STRING);
      req.client_context_id = cb_string_new(client_context_id);
    }
    cb_extract_timeout(req, options);
    cb_extract_option_bool(req.adhoc, options, "adhoc");
    cb_extract_option_bool(req.metrics, options, "metrics");
    cb_extract_option_bool(req.readonly, options, "readonly");
    cb_extract_option_bool(req.flex_index, options, "flex_index");
    cb_extract_option_bool(req.preserve_expiry, options, "preserve_expiry");
    cb_extract_option_bool(req.use_replica, options, "use_replica");
    cb_extract_option_uint64(req.scan_cap, options, "scan_cap");
    cb_extract_duration(req.scan_wait, options, "scan_wait");
    cb_extract_option_uint64(req.max_parallelism, options, "max_parallelism");
    cb_extract_option_uint64(req.pipeline_cap, options, "pipeline_cap");
    cb_extract_option_uint64(req.pipeline_batch, options, "pipeline_batch");
    if (VALUE query_context = rb_hash_aref(options, rb_id2sym(rb_intern("query_context")));
        !NIL_P(query_context) && TYPE(query_context) == T_STRING) {
      req.query_context.emplace(cb_string_new(query_context));
    }
    if (VALUE profile = rb_hash_aref(options, rb_id2sym(rb_intern("profile"))); !NIL_P(profile)) {
      cb_check_type(profile, T_SYMBOL);
      ID mode = rb_sym2id(profile);
      if (mode == rb_intern("phases")) {
        req.profile = couchbase::query_profile::phases;
      } else if (mode == rb_intern("timings")) {
        req.profile = couchbase::query_profile::timings;
      } else if (mode == rb_intern("off")) {
        req.profile = couchbase::query_profile::off;
      }
    }
    if (VALUE positional_params =
          rb_hash_aref(options, rb_id2sym(rb_intern("positional_parameters")));
        !NIL_P(positional_params)) {
      cb_check_type(positional_params, T_ARRAY);
      auto entries_num = static_cast<std::size_t>(RARRAY_LEN(positional_params));
      req.positional_parameters.reserve(entries_num);
      for (std::size_t i = 0; i < entries_num; ++i) {
        VALUE entry = rb_ary_entry(positional_params, static_cast<long>(i));
        cb_check_type(entry, T_STRING);
        req.positional_parameters.emplace_back(cb_string_new(entry));
      }
    }
    if (VALUE named_params = rb_hash_aref(options, rb_id2sym(rb_intern("named_parameters")));
        !NIL_P(named_params)) {
      cb_check_type(named_params, T_HASH);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      rb_hash_foreach(named_params, cb_for_each_named_param, reinterpret_cast<VALUE>(&req));
    }
    if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency")));
        !NIL_P(scan_consistency)) {
      cb_check_type(scan_consistency, T_SYMBOL);
      ID type = rb_sym2id(scan_consistency);
      if (type == rb_intern("not_bounded")) {
        req.scan_consistency = couchbase::query_scan_consistency::not_bounded;
      } else if (type == rb_intern("request_plus")) {
        req.scan_consistency = couchbase::query_scan_consistency::request_plus;
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
            rb_raise(rb_eArgError, "partition_uuid must be an Integer");
        }
        VALUE sequence_number = rb_hash_aref(token, rb_id2sym(rb_intern("sequence_number")));
        switch (TYPE(sequence_number)) {
          case T_FIXNUM:
          case T_BIGNUM:
            break;
          default:
            rb_raise(rb_eArgError, "sequence_number must be an Integer");
        }
        req.mutation_state.emplace_back(NUM2ULL(partition_uuid),
                                        NUM2ULL(sequence_number),
                                        gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
                                        cb_string_new(bucket_name));
      }
    }

    if (VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters")));
        !NIL_P(raw_params)) {
      cb_check_type(raw_params, T_HASH);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      rb_hash_foreach(raw_params, cb_for_each_raw_param, reinterpret_cast<VALUE>(&req));
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::query_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (resp.meta.errors && !resp.meta.errors->empty()) {
        const auto& first_error = resp.meta.errors->front();
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to query ({}: {}))", first_error.code, first_error.message));
      } else {
        cb_throw_error(resp.ctx, "unable to query");
      }
    }
    VALUE res = rb_hash_new();
    VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
    rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);
    for (const auto& row : resp.rows) {
      rb_ary_push(rows, cb_str_new(row));
    }
    VALUE meta = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);
    rb_hash_aset(
      meta,
      rb_id2sym(rb_intern("status")),
      rb_id2sym(rb_intern2(resp.meta.status.data(), static_cast<long>(resp.meta.status.size()))));
    rb_hash_aset(meta, rb_id2sym(rb_intern("request_id")), cb_str_new(resp.meta.request_id));
    rb_hash_aset(
      meta, rb_id2sym(rb_intern("client_context_id")), cb_str_new(resp.meta.client_context_id));
    if (resp.meta.signature) {
      rb_hash_aset(
        meta, rb_id2sym(rb_intern("signature")), cb_str_new(resp.meta.signature.value()));
    }
    if (resp.meta.profile) {
      rb_hash_aset(meta, rb_id2sym(rb_intern("profile")), cb_str_new(resp.meta.profile.value()));
    }
    if (resp.meta.metrics) {
      VALUE metrics = rb_hash_new();
      rb_hash_aset(meta, rb_id2sym(rb_intern("metrics")), metrics);
      rb_hash_aset(metrics,
                   rb_id2sym(rb_intern("elapsed_time")),
                   ULL2NUM(resp.meta.metrics->elapsed_time.count()));
      rb_hash_aset(metrics,
                   rb_id2sym(rb_intern("execution_time")),
                   ULL2NUM(resp.meta.metrics->execution_time.count()));
      rb_hash_aset(
        metrics, rb_id2sym(rb_intern("result_count")), ULL2NUM(resp.meta.metrics->result_count));
      rb_hash_aset(
        metrics, rb_id2sym(rb_intern("result_size")), ULL2NUM(resp.meta.metrics->result_size));
      rb_hash_aset(
        metrics, rb_id2sym(rb_intern("sort_count")), ULL2NUM(resp.meta.metrics->sort_count));
      rb_hash_aset(metrics,
                   rb_id2sym(rb_intern("mutation_count")),
                   ULL2NUM(resp.meta.metrics->mutation_count));
      rb_hash_aset(
        metrics, rb_id2sym(rb_intern("error_count")), ULL2NUM(resp.meta.metrics->error_count));
      rb_hash_aset(
        metrics, rb_id2sym(rb_intern("warning_count")), ULL2NUM(resp.meta.metrics->warning_count));
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

VALUE
cb_Backend_collection_query_index_get_all(VALUE self,
                                          VALUE bucket_name,
                                          VALUE scope_name,
                                          VALUE collection_name,
                                          VALUE options,
                                          VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_get_all_request req{};
    req.bucket_name = cb_string_new(bucket_name);
    req.scope_name = cb_string_new(scope_name);
    req.collection_name = cb_string_new(collection_name);
    cb_extract_timeout(req, options);
    auto parent_span = cb_create_parent_span(req, self);
    std::promise<core::operations::management::query_index_get_all_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format("unable to get list of the indexes of the collection \"{}\"",
                                 req.collection_name));
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    VALUE indexes = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
    for (const auto& idx : resp.indexes) {
      VALUE index = rb_hash_new();
      rb_hash_aset(index, rb_id2sym(rb_intern("state")), rb_id2sym(rb_intern(idx.state.c_str())));
      rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
      rb_hash_aset(index, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern(idx.type.c_str())));
      rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
      VALUE index_key = rb_ary_new_capa(static_cast<long>(idx.index_key.size()));
      for (const auto& key : idx.index_key) {
        rb_ary_push(index_key, cb_str_new(key));
      }
      rb_hash_aset(index, rb_id2sym(rb_intern("index_key")), index_key);
      if (idx.collection_name) {
        rb_hash_aset(
          index, rb_id2sym(rb_intern("collection_name")), cb_str_new(idx.collection_name.value()));
      }
      if (idx.scope_name) {
        rb_hash_aset(index, rb_id2sym(rb_intern("scope_name")), cb_str_new(idx.scope_name.value()));
      }
      rb_hash_aset(index, rb_id2sym(rb_intern("bucket_name")), cb_str_new(idx.bucket_name));
      if (idx.condition) {
        rb_hash_aset(index, rb_id2sym(rb_intern("condition")), cb_str_new(idx.condition.value()));
      }
      if (idx.partition) {
        rb_hash_aset(index, rb_id2sym(rb_intern("partition")), cb_str_new(idx.partition.value()));
      }
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
cb_Backend_collection_query_index_create(VALUE self,
                                         VALUE bucket_name,
                                         VALUE scope_name,
                                         VALUE collection_name,
                                         VALUE index_name,
                                         VALUE keys,
                                         VALUE options,
                                         VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);
  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  Check_Type(index_name, T_STRING);
  Check_Type(keys, T_ARRAY);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_create_request req{};
    cb_extract_timeout(req, options);
    req.bucket_name = cb_string_new(bucket_name);
    req.scope_name = cb_string_new(scope_name);
    req.collection_name = cb_string_new(collection_name);
    req.index_name = cb_string_new(index_name);
    auto keys_num = static_cast<std::size_t>(RARRAY_LEN(keys));
    req.keys.reserve(keys_num);
    for (std::size_t i = 0; i < keys_num; ++i) {
      VALUE entry = rb_ary_entry(keys, static_cast<long>(i));
      cb_check_type(entry, T_STRING);
      req.keys.emplace_back(RSTRING_PTR(entry), static_cast<std::size_t>(RSTRING_LEN(entry)));
    }
    if (!NIL_P(options)) {
      if (VALUE ignore_if_exists = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_exists")));
          ignore_if_exists == Qtrue) {
        req.ignore_if_exists = true;
      } else if (ignore_if_exists == Qfalse) {
        req.ignore_if_exists = false;
      } /* else use backend default */
      if (VALUE deferred = rb_hash_aref(options, rb_id2sym(rb_intern("deferred")));
          deferred == Qtrue) {
        req.deferred = true;
      } else if (deferred == Qfalse) {
        req.deferred = false;
      } /* else use backend default */
      if (VALUE num_replicas = rb_hash_aref(options, rb_id2sym(rb_intern("num_replicas")));
          !NIL_P(num_replicas)) {
        req.num_replicas = NUM2UINT(num_replicas);
      } /* else use backend default */
      if (VALUE condition = rb_hash_aref(options, rb_id2sym(rb_intern("condition")));
          !NIL_P(condition)) {
        req.condition.emplace(cb_string_new(condition));
      } /* else use backend default */
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_create_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to create index "{}" on the collection "{}" ({}: {}))",
                                   req.index_name,
                                   req.collection_name,
                                   first_error.code,
                                   first_error.message));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to create index "{}" on the collection "{}")",
                                   req.index_name,
                                   req.collection_name));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

VALUE
cb_Backend_collection_query_index_drop(VALUE self,
                                       VALUE bucket_name,
                                       VALUE scope_name,
                                       VALUE collection_name,
                                       VALUE index_name,
                                       VALUE options,
                                       VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  Check_Type(index_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_drop_request req{};
    cb_extract_timeout(req, options);
    req.bucket_name = cb_string_new(bucket_name);
    req.scope_name = cb_string_new(scope_name);
    req.collection_name = cb_string_new(collection_name);
    req.index_name = cb_string_new(index_name);
    if (!NIL_P(options)) {
      if (VALUE ignore_if_does_not_exist =
            rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_does_not_exist")));
          ignore_if_does_not_exist == Qtrue) {
        req.ignore_if_does_not_exist = true;
      } else if (ignore_if_does_not_exist == Qfalse) {
        req.ignore_if_does_not_exist = false;
      } /* else use backend default */
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_drop_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to drop index "{}" on the collection "{}" ({}: {}))",
                                   req.index_name,
                                   req.collection_name,
                                   first_error.code,
                                   first_error.message));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to drop index "{}" on the collection "{}")",
                                   req.index_name,
                                   req.collection_name));
      }
    }
    VALUE res = rb_hash_new();

    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

VALUE
cb_Backend_collection_query_index_create_primary(VALUE self,
                                                 VALUE bucket_name,
                                                 VALUE scope_name,
                                                 VALUE collection_name,
                                                 VALUE options,
                                                 VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_create_request req{};
    cb_extract_timeout(req, options);
    req.is_primary = true;
    req.bucket_name = cb_string_new(bucket_name);
    req.scope_name = cb_string_new(scope_name);
    req.collection_name = cb_string_new(collection_name);
    if (!NIL_P(options)) {
      if (VALUE ignore_if_exists = rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_exists")));
          ignore_if_exists == Qtrue) {
        req.ignore_if_exists = true;
      } else if (ignore_if_exists == Qfalse) {
        req.ignore_if_exists = false;
      } /* else use backend default */
      if (VALUE deferred = rb_hash_aref(options, rb_id2sym(rb_intern("deferred")));
          deferred == Qtrue) {
        req.deferred = true;
      } else if (deferred == Qfalse) {
        req.deferred = false;
      } /* else use backend default */
      if (VALUE num_replicas = rb_hash_aref(options, rb_id2sym(rb_intern("num_replicas")));
          !NIL_P(num_replicas)) {
        req.num_replicas = NUM2UINT(num_replicas);
      } /* else use backend default */
      if (VALUE index_name = rb_hash_aref(options, rb_id2sym(rb_intern("index_name")));
          TYPE(index_name) == T_STRING) {
        req.index_name = cb_string_new(index_name);
      } /* else use backend default */
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_create_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to create primary index on the collection "{}" ({}: {}))",
                      req.collection_name,
                      first_error.code,
                      first_error.message));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to create primary index on the collection "{}")",
                                   req.collection_name));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

VALUE
cb_Backend_collection_query_index_drop_primary(VALUE self,
                                               VALUE bucket_name,
                                               VALUE scope_name,
                                               VALUE collection_name,
                                               VALUE options,
                                               VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_drop_request req{};
    cb_extract_timeout(req, options);
    req.is_primary = true;
    req.bucket_name = cb_string_new(bucket_name);
    req.scope_name = cb_string_new(scope_name);
    req.collection_name = cb_string_new(collection_name);
    if (!NIL_P(options)) {
      if (VALUE ignore_if_does_not_exist =
            rb_hash_aref(options, rb_id2sym(rb_intern("ignore_if_does_not_exist")));
          ignore_if_does_not_exist == Qtrue) {
        req.ignore_if_does_not_exist = true;
      } else if (ignore_if_does_not_exist == Qfalse) {
        req.ignore_if_does_not_exist = false;
      } /* else use backend default */
      if (VALUE index_name = rb_hash_aref(options, rb_id2sym(rb_intern("index_name")));
          !NIL_P(index_name)) {
        cb_check_type(options, T_STRING);
        req.is_primary = false;
        req.bucket_name = cb_string_new(index_name);
      }
      if (VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
          TYPE(scope_name) == T_STRING) {
        req.scope_name = cb_string_new(scope_name);
      }
      if (VALUE collection_name = rb_hash_aref(options, rb_id2sym(rb_intern("collection_name")));
          TYPE(collection_name) == T_STRING) {
        req.collection_name = cb_string_new(collection_name);
      }
    }
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_drop_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to drop primary index on the collection "{}" ({}: {}))",
                      req.collection_name,
                      first_error.code,
                      first_error.message));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to drop primary index on the collection "{}")",
                                   req.collection_name));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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

VALUE
cb_Backend_collection_query_index_build_deferred(VALUE self,
                                                 VALUE bucket_name,
                                                 VALUE scope_name,
                                                 VALUE collection_name,
                                                 VALUE options,
                                                 VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::query_index_build_deferred_request req{};
    cb_extract_timeout(req, options);
    req.bucket_name = cb_string_new(bucket_name);
    req.scope_name = cb_string_new(scope_name);
    req.collection_name = cb_string_new(collection_name);
    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::management::query_index_build_deferred_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts);
    if (resp.ctx.ec) {
      if (!resp.errors.empty()) {
        const auto& first_error = resp.errors.front();
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to build deferred indexes on the collection "{}" ({}: {}))",
                      req.collection_name.value(),
                      first_error.code,
                      first_error.message));
      } else {
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to build deferred indexes on the collection "{}")",
                                   req.collection_name.value()));
      }
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("status")), cb_str_new(resp.status));
    if (!resp.errors.empty()) {
      VALUE errors = rb_ary_new_capa(static_cast<long>(resp.errors.size()));
      for (const auto& err : resp.errors) {
        VALUE error = rb_hash_new();
        rb_hash_aset(error, rb_id2sym(rb_intern("code")), ULL2NUM(err.code));
        rb_hash_aset(error, rb_id2sym(rb_intern("message")), cb_str_new(err.message));
        rb_ary_push(errors, error);
      }
      rb_hash_aset(res, rb_id2sym(rb_intern("errors")), errors);
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
init_query(VALUE cBackend)
{
  rb_define_method(cBackend, "document_query", cb_Backend_document_query, 3);

  rb_define_method(cBackend, "query_index_get_all", cb_Backend_query_index_get_all, 3);
  rb_define_method(cBackend, "query_index_create", cb_Backend_query_index_create, 5);
  rb_define_method(
    cBackend, "query_index_create_primary", cb_Backend_query_index_create_primary, 3);
  rb_define_method(cBackend, "query_index_drop", cb_Backend_query_index_drop, 4);
  rb_define_method(cBackend, "query_index_drop_primary", cb_Backend_query_index_drop_primary, 3);
  rb_define_method(
    cBackend, "query_index_build_deferred", cb_Backend_query_index_build_deferred, 3);

  rb_define_method(
    cBackend, "collection_query_index_get_all", cb_Backend_collection_query_index_get_all, 5);
  rb_define_method(
    cBackend, "collection_query_index_create", cb_Backend_collection_query_index_create, 7);
  rb_define_method(cBackend,
                   "collection_query_index_create_primary",
                   cb_Backend_collection_query_index_create_primary,
                   5);
  rb_define_method(
    cBackend, "collection_query_index_drop", cb_Backend_collection_query_index_drop, 6);
  rb_define_method(cBackend,
                   "collection_query_index_drop_primary",
                   cb_Backend_collection_query_index_drop_primary,
                   5);
  rb_define_method(cBackend,
                   "collection_query_index_build_deferred",
                   cb_Backend_collection_query_index_build_deferred,
                   5);
}
} // namespace couchbase::ruby
