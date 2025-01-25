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
#include <core/operations/management/collection_create.hxx>
#include <core/operations/management/collection_drop.hxx>
#include <core/operations/management/collection_update.hxx>
#include <core/operations/management/scope_create.hxx>
#include <core/operations/management/scope_drop.hxx>
#include <core/operations/management/scope_get_all.hxx>

#include <spdlog/fmt/bundled/core.h>

#include <future>
#include <memory>

#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{
VALUE
cb_Backend_scope_get_all(VALUE self, VALUE bucket_name, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::scope_get_all_request req{ cb_string_new(bucket_name) };
    cb_extract_timeout(req, options);
    std::promise<core::operations::management::scope_get_all_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(
        resp.ctx,
        fmt::format("unable to get list of the scopes of the bucket \"{}\"", req.bucket_name));
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
        rb_hash_aset(collection, rb_id2sym(rb_intern("max_expiry")), LONG2NUM(c.max_expiry));
        if (c.history.has_value()) {
          rb_hash_aset(
            collection, rb_id2sym(rb_intern("history")), c.history.value() ? Qtrue : Qfalse);
        }
        rb_ary_push(collections, collection);
      }
      rb_hash_aset(scope, rb_id2sym(rb_intern("collections")), collections);
      rb_ary_push(scopes, scope);
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("scopes")), scopes);

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
cb_Backend_scope_create(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::scope_create_request req{ cb_string_new(bucket_name),
                                                            cb_string_new(scope_name) };
    cb_extract_timeout(req, options);
    std::promise<core::operations::management::scope_create_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);

    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable to create the scope "{}" on the bucket "{}")",
                                 req.scope_name,
                                 req.bucket_name));
    }
    return ULL2NUM(resp.uid);
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_scope_drop(VALUE self, VALUE bucket_name, VALUE scope_name, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::scope_drop_request req{ cb_string_new(bucket_name),
                                                          cb_string_new(scope_name) };
    cb_extract_timeout(req, options);
    std::promise<core::operations::management::scope_drop_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable to drop the scope "{}" on the bucket "{}")",
                                 req.scope_name,
                                 req.bucket_name));
    }
    return ULL2NUM(resp.uid);
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_collection_create(VALUE self,
                             VALUE bucket_name,
                             VALUE scope_name,
                             VALUE collection_name,
                             VALUE settings,
                             VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  if (!NIL_P(settings)) {
    Check_Type(settings, T_HASH);
  }
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::collection_create_request req{ cb_string_new(bucket_name),
                                                                 cb_string_new(scope_name),
                                                                 cb_string_new(collection_name) };
    cb_extract_timeout(req, options);

    if (!NIL_P(settings)) {
      if (VALUE max_expiry = rb_hash_aref(settings, rb_id2sym(rb_intern("max_expiry")));
          !NIL_P(max_expiry)) {
        if (TYPE(max_expiry) == T_FIXNUM) {
          req.max_expiry = FIX2INT(max_expiry);
          if (req.max_expiry < -1) {
            throw ruby_exception(
              exc_invalid_argument(),
              rb_sprintf(
                "collection max expiry must be greater than or equal to -1, given %+" PRIsVALUE,
                max_expiry));
          }
        } else {
          throw ruby_exception(
            rb_eArgError,
            rb_sprintf("collection max expiry must be an Integer, given %+" PRIsVALUE, max_expiry));
        }
      }
      if (VALUE history = rb_hash_aref(settings, rb_id2sym(rb_intern("history")));
          !NIL_P(history)) {
        req.history = RTEST(history);
      }
    }

    std::promise<core::operations::management::collection_create_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable create the collection "{}.{}" on the bucket "{}")",
                                 req.scope_name,
                                 req.collection_name,
                                 req.bucket_name));
    }
    return ULL2NUM(resp.uid);
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_collection_update(VALUE self,
                             VALUE bucket_name,
                             VALUE scope_name,
                             VALUE collection_name,
                             VALUE settings,
                             VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  if (!NIL_P(settings)) {
    Check_Type(settings, T_HASH);
  }
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::collection_update_request req{ cb_string_new(bucket_name),
                                                                 cb_string_new(scope_name),
                                                                 cb_string_new(collection_name) };
    cb_extract_timeout(req, options);

    if (!NIL_P(settings)) {
      if (VALUE max_expiry = rb_hash_aref(settings, rb_id2sym(rb_intern("max_expiry")));
          !NIL_P(max_expiry)) {
        if (TYPE(max_expiry) == T_FIXNUM) {
          req.max_expiry = FIX2INT(max_expiry);
          if (req.max_expiry < -1) {
            throw ruby_exception(
              exc_invalid_argument(),
              rb_sprintf(
                "collection max expiry must be greater than or equal to -1, given %+" PRIsVALUE,
                max_expiry));
          }
        } else {
          throw ruby_exception(
            rb_eArgError,
            rb_sprintf("collection max expiry must be an Integer, given %+" PRIsVALUE, max_expiry));
        }
      }
      if (VALUE history = rb_hash_aref(settings, rb_id2sym(rb_intern("history")));
          !NIL_P(history)) {
        req.history = RTEST(history);
      }
    }

    std::promise<core::operations::management::collection_update_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable update the collection "{}.{}" on the bucket "{}")",
                                 req.scope_name,
                                 req.collection_name,
                                 req.bucket_name));
    }
    return ULL2NUM(resp.uid);
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_collection_drop(VALUE self,
                           VALUE bucket_name,
                           VALUE scope_name,
                           VALUE collection_name,
                           VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(scope_name, T_STRING);
  Check_Type(collection_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::collection_drop_request req{ cb_string_new(bucket_name),
                                                               cb_string_new(scope_name),
                                                               cb_string_new(collection_name) };
    cb_extract_timeout(req, options);

    std::promise<core::operations::management::collection_drop_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable to drop the collection  "{}.{}" on the bucket "{}")",
                                 req.scope_name,
                                 req.collection_name,
                                 req.bucket_name));
    }

    return ULL2NUM(resp.uid);
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
init_collections(VALUE cBackend)
{
  rb_define_method(cBackend, "scope_get_all", cb_Backend_scope_get_all, 2);
  rb_define_method(cBackend, "scope_create", cb_Backend_scope_create, 3);
  rb_define_method(cBackend, "scope_drop", cb_Backend_scope_drop, 3);
  rb_define_method(cBackend, "collection_create", cb_Backend_collection_create, 5);
  rb_define_method(cBackend, "collection_update", cb_Backend_collection_update, 5);
  rb_define_method(cBackend, "collection_drop", cb_Backend_collection_drop, 4);
}
} // namespace couchbase::ruby
