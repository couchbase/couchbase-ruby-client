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
#include <core/impl/subdoc/path_flags.hxx>
#include <core/operations/document_append.hxx>
#include <core/operations/document_decrement.hxx>
#include <core/operations/document_exists.hxx>
#include <core/operations/document_get.hxx>
#include <core/operations/document_get_all_replicas.hxx>
#include <core/operations/document_get_and_lock.hxx>
#include <core/operations/document_get_and_touch.hxx>
#include <core/operations/document_get_any_replica.hxx>
#include <core/operations/document_get_projected.hxx>
#include <core/operations/document_increment.hxx>
#include <core/operations/document_insert.hxx>
#include <core/operations/document_lookup_in.hxx>
#include <core/operations/document_lookup_in_all_replicas.hxx>
#include <core/operations/document_lookup_in_any_replica.hxx>
#include <core/operations/document_mutate_in.hxx>
#include <core/operations/document_prepend.hxx>
#include <core/operations/document_remove.hxx>
#include <core/operations/document_replace.hxx>
#include <core/operations/document_touch.hxx>
#include <core/operations/document_unlock.hxx>
#include <core/operations/document_upsert.hxx>
#include <core/utils/json.hxx>

#include <couchbase/cluster.hxx>
#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/codec/transcoder_traits.hxx>

#include <future>

#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_observability.hxx"
#include "rcb_utils.hxx"

namespace couchbase
{
namespace ruby
{
struct passthrough_transcoder {
  using document_type = codec::encoded_value;

  static auto decode(const codec::encoded_value& data) -> document_type
  {
    return data;
  }

  static auto encode(codec::encoded_value document) -> codec::encoded_value
  {
    return document;
  }
};
} // namespace ruby

template<>
struct codec::is_transcoder<ruby::passthrough_transcoder> : public std::true_type {
};
}; // namespace couchbase

namespace couchbase::ruby
{
namespace
{
VALUE
cb_Backend_document_get(VALUE self,
                        VALUE bucket,
                        VALUE scope,
                        VALUE collection,
                        VALUE id,
                        VALUE options,
                        VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::get_request req{ doc_id };
    cb_extract_timeout(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::get_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to fetch document");
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
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
cb_Backend_document_get_any_replica(VALUE self,
                                    VALUE bucket,
                                    VALUE scope,
                                    VALUE collection,
                                    VALUE id,
                                    VALUE options,
                                    VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::get_any_replica_request req{ doc_id };
    cb_extract_timeout(req, options);
    cb_extract_read_preference(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::get_any_replica_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to get replica of the document");
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
    rb_hash_aset(res, rb_id2sym(rb_intern("replica")), resp.replica ? Qtrue : Qfalse);
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
cb_Backend_document_get_all_replicas(VALUE self,
                                     VALUE bucket,
                                     VALUE scope,
                                     VALUE collection,
                                     VALUE id,
                                     VALUE options,
                                     VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::get_all_replicas_request req{ doc_id };
    cb_extract_timeout(req, options);
    cb_extract_read_preference(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::get_all_replicas_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to get all replicas for the document");
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(resp.entries.size()));

    for (const auto& entry : resp.entries) {
      VALUE response = rb_hash_new();
      rb_hash_aset(response, rb_id2sym(rb_intern("content")), cb_str_new(entry.value));
      rb_hash_aset(response, rb_id2sym(rb_intern("cas")), cb_cas_to_num(entry.cas));
      rb_hash_aset(response, rb_id2sym(rb_intern("flags")), UINT2NUM(entry.flags));
      rb_hash_aset(response, rb_id2sym(rb_intern("replica")), entry.replica ? Qtrue : Qfalse);
      rb_ary_push(res, response);
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
cb_Backend_document_get_projected(VALUE self,
                                  VALUE bucket,
                                  VALUE scope,
                                  VALUE collection,
                                  VALUE id,
                                  VALUE options,
                                  VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::get_projected_request req{ doc_id };
    cb_extract_timeout(req, options);
    auto parent_span = cb_create_parent_span(req, self);
    cb_extract_option_bool(req.with_expiry, options, "with_expiry");
    cb_extract_option_bool(req.preserve_array_indexes, options, "preserve_array_indexes");
    VALUE projections = Qnil;
    cb_extract_option_array(projections, options, "projections");
    if (!NIL_P(projections)) {
      auto entries_num = static_cast<std::size_t>(RARRAY_LEN(projections));
      if (entries_num == 0) {
        throw ruby_exception(rb_eArgError, "projections array must not be empty");
      }
      req.projections.reserve(entries_num);
      for (std::size_t i = 0; i < entries_num; ++i) {
        VALUE entry = rb_ary_entry(projections, static_cast<long>(i));
        cb_check_type(entry, T_STRING);
        req.projections.emplace_back(cb_string_new(entry));
      }
    }

    std::promise<core::operations::get_projected_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable fetch with projections");
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
    if (resp.expiry) {
      rb_hash_aset(res, rb_id2sym(rb_intern("expiry")), UINT2NUM(resp.expiry.value()));
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
cb_Backend_document_get_and_lock(VALUE self,
                                 VALUE bucket,
                                 VALUE scope,
                                 VALUE collection,
                                 VALUE id,
                                 VALUE lock_time,
                                 VALUE options,
                                 VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(lock_time, T_FIXNUM);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::get_and_lock_request req{ doc_id };
    cb_extract_timeout(req, options);
    req.lock_time = NUM2UINT(lock_time);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::get_and_lock_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable lock and fetch");
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
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
cb_Backend_document_get_and_touch(VALUE self,
                                  VALUE bucket,
                                  VALUE scope,
                                  VALUE collection,
                                  VALUE id,
                                  VALUE expiry,
                                  VALUE options,
                                  VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::get_and_touch_request req{ doc_id };
    cb_extract_timeout(req, options);
    auto [type, duration] = unpack_expiry(expiry, false);
    req.expiry = static_cast<std::uint32_t>(duration.count());

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::get_and_touch_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable fetch and touch");
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
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
cb_Backend_document_touch(VALUE self,
                          VALUE bucket,
                          VALUE scope,
                          VALUE collection,
                          VALUE id,
                          VALUE expiry,
                          VALUE options,
                          VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::touch_request req{ doc_id };
    cb_extract_timeout(req, options);
    auto [type, duration] = unpack_expiry(expiry, false);
    req.expiry = static_cast<std::uint32_t>(duration.count());

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::touch_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to touch");
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
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
cb_Backend_document_exists(VALUE self,
                           VALUE bucket,
                           VALUE scope,
                           VALUE collection,
                           VALUE id,
                           VALUE options,
                           VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::exists_request req{ doc_id };
    cb_extract_timeout(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::exists_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec() && resp.ctx.ec() != couchbase::errc::key_value::document_not_found) {
      cb_throw_error(resp.ctx, "unable to exists");
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
    rb_hash_aset(res, rb_id2sym(rb_intern("exists")), resp.exists() ? Qtrue : Qfalse);
    rb_hash_aset(res, rb_id2sym(rb_intern("deleted")), resp.deleted ? Qtrue : Qfalse);
    rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
    rb_hash_aset(res, rb_id2sym(rb_intern("expiry")), UINT2NUM(resp.expiry));
    rb_hash_aset(res, rb_id2sym(rb_intern("sequence_number")), ULL2NUM(resp.sequence_number));
    rb_hash_aset(res, rb_id2sym(rb_intern("datatype")), UINT2NUM(resp.datatype));
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
cb_Backend_document_unlock(VALUE self,
                           VALUE bucket,
                           VALUE scope,
                           VALUE collection,
                           VALUE id,
                           VALUE cas,
                           VALUE options,
                           VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::unlock_request req{ doc_id };
    cb_extract_timeout(req, options);
    cb_extract_cas(req.cas, cas);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::unlock_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to unlock");
    }

    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
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
cb_Backend_document_upsert(VALUE self,
                           VALUE bucket,
                           VALUE scope,
                           VALUE collection,
                           VALUE id,
                           VALUE content,
                           VALUE flags,
                           VALUE options,
                           VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(content, T_STRING);
  Check_Type(flags, T_FIXNUM);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::upsert_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };
    cb_extract_content(req, content);
    cb_extract_flags(req, flags);
    cb_extract_timeout(req, options);
    cb_extract_expiry(req, options);
    cb_extract_durability_level(req, options);
    cb_extract_preserve_expiry(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::upsert_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::upsert_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }

    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to upsert");
    }

    return cb_create_mutation_result(resp);

  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_document_append(VALUE self,
                           VALUE bucket,
                           VALUE scope,
                           VALUE collection,
                           VALUE id,
                           VALUE content,
                           VALUE options,
                           VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(content, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::append_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };
    cb_extract_content(req, content);
    cb_extract_timeout(req, options);
    cb_extract_durability_level(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::append_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::append_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }

    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to append");
    }

    return cb_create_mutation_result(resp);

  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_document_prepend(VALUE self,
                            VALUE bucket,
                            VALUE scope,
                            VALUE collection,
                            VALUE id,
                            VALUE content,
                            VALUE options,
                            VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(content, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::prepend_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };
    cb_extract_content(req, content);
    cb_extract_timeout(req, options);
    cb_extract_durability_level(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::prepend_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::prepend_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }

    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to prepend");
    }

    return cb_create_mutation_result(resp);
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_document_replace(VALUE self,
                            VALUE bucket,
                            VALUE scope,
                            VALUE collection,
                            VALUE id,
                            VALUE content,
                            VALUE flags,
                            VALUE options,
                            VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(content, T_STRING);
  Check_Type(flags, T_FIXNUM);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::replace_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };
    cb_extract_content(req, content);
    cb_extract_flags(req, flags);
    cb_extract_timeout(req, options);
    cb_extract_expiry(req, options);
    cb_extract_durability_level(req, options);
    cb_extract_preserve_expiry(req, options);
    cb_extract_cas(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::replace_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::replace_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }

    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to replace");
    }
    return cb_create_mutation_result(resp);

  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_document_insert(VALUE self,
                           VALUE bucket,
                           VALUE scope,
                           VALUE collection,
                           VALUE id,
                           VALUE content,
                           VALUE flags,
                           VALUE options,
                           VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(content, T_STRING);
  Check_Type(flags, T_FIXNUM);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::insert_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };
    cb_extract_content(req, content);
    cb_extract_flags(req, flags);
    cb_extract_timeout(req, options);
    cb_extract_expiry(req, options);
    cb_extract_durability_level(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::insert_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::insert_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to insert");
    }
    return cb_create_mutation_result(resp);

  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_document_remove(VALUE self,
                           VALUE bucket,
                           VALUE scope,
                           VALUE collection,
                           VALUE id,
                           VALUE options,
                           VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::remove_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };
    cb_extract_timeout(req, options);
    cb_extract_durability_level(req, options);
    cb_extract_cas(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::remove_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::remove_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }

    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to remove");
    }
    return cb_create_mutation_result(resp);
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_document_increment(VALUE self,
                              VALUE bucket,
                              VALUE scope,
                              VALUE collection,
                              VALUE id,
                              VALUE options,
                              VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::increment_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };

    cb_extract_timeout(req, options);
    cb_extract_expiry(req, options);
    cb_extract_option_uint64(req.delta, options, "delta");
    cb_extract_option_uint64(req.initial_value, options, "initial_value");
    cb_extract_durability_level(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::increment_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::increment_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }

    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to increment");
    }

    VALUE res = cb_create_mutation_result(resp);
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content));
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
cb_Backend_document_decrement(VALUE self,
                              VALUE bucket,
                              VALUE scope,
                              VALUE collection,
                              VALUE id,
                              VALUE options,
                              VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::decrement_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };

    cb_extract_timeout(req, options);
    cb_extract_expiry(req, options);
    cb_extract_option_uint64(req.delta, options, "delta");
    cb_extract_option_uint64(req.initial_value, options, "initial_value");
    cb_extract_durability_level(req, options);

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::decrement_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::decrement_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }

    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to decrement");
    }

    VALUE res = cb_create_mutation_result(resp);
    rb_hash_aset(res, rb_id2sym(rb_intern("content")), ULL2NUM(resp.content));
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
cb_Backend_document_lookup_in(VALUE self,
                              VALUE bucket,
                              VALUE scope,
                              VALUE collection,
                              VALUE id,
                              VALUE specs,
                              VALUE options,
                              VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(specs, T_ARRAY);
  if (RARRAY_LEN(specs) <= 0) {
    rb_raise(rb_eArgError, "Array with specs cannot be empty");
    return Qnil;
  }
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::lookup_in_request req{ doc_id };
    cb_extract_timeout(req, options);
    cb_extract_option_bool(req.access_deleted, options, "access_deleted");

    static VALUE xattr_property = rb_id2sym(rb_intern("xattr"));
    static VALUE path_property = rb_id2sym(rb_intern("path"));
    static VALUE opcode_property = rb_id2sym(rb_intern("opcode"));

    auto entries_size = static_cast<std::size_t>(RARRAY_LEN(specs));
    for (std::size_t i = 0; i < entries_size; ++i) {
      VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
      cb_check_type(entry, T_HASH);
      VALUE operation = rb_hash_aref(entry, opcode_property);
      cb_check_type(operation, T_SYMBOL);
      bool xattr = RTEST(rb_hash_aref(entry, xattr_property));
      VALUE path = rb_hash_aref(entry, path_property);
      cb_check_type(path, T_STRING);
      auto opcode = core::impl::subdoc::opcode{};
      if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("get_doc")) {
        opcode = core::impl::subdoc::opcode::get_doc;
      } else if (operation_id == rb_intern("get")) {
        opcode = core::impl::subdoc::opcode::get;
      } else if (operation_id == rb_intern("exists")) {
        opcode = core::impl::subdoc::opcode::exists;
      } else if (operation_id == rb_intern("count")) {
        opcode = core::impl::subdoc::opcode::get_count;
      } else {
        throw ruby_exception(
          exc_invalid_argument(),
          rb_sprintf("unsupported operation for subdocument lookup: %+" PRIsVALUE, operation));
      }
      cb_check_type(path, T_STRING);

      req.specs.emplace_back(core::impl::subdoc::command{
        opcode,
        cb_string_new(path),
        {},
        core::impl::subdoc::build_lookup_in_path_flags(xattr, false) });
    }

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::lookup_in_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to perform lookup_in operation");
    }

    static VALUE deleted_property = rb_id2sym(rb_intern("deleted"));
    static VALUE fields_property = rb_id2sym(rb_intern("fields"));
    static VALUE index_property = rb_id2sym(rb_intern("index"));
    static VALUE exists_property = rb_id2sym(rb_intern("exists"));
    static VALUE cas_property = rb_id2sym(rb_intern("cas"));
    static VALUE value_property = rb_id2sym(rb_intern("value"));
    static VALUE error_property = rb_id2sym(rb_intern("error"));

    VALUE res = rb_hash_new();
    rb_hash_aset(res, cas_property, cb_cas_to_num(resp.cas));
    VALUE fields = rb_ary_new_capa(static_cast<long>(entries_size));
    rb_hash_aset(res, fields_property, fields);
    rb_hash_aset(res, deleted_property, resp.deleted ? Qtrue : Qfalse);
    for (std::size_t i = 0; i < entries_size; ++i) {
      auto resp_entry = resp.fields.at(i);
      VALUE entry = rb_hash_new();
      rb_hash_aset(entry, index_property, ULL2NUM(resp_entry.original_index));
      rb_hash_aset(entry, exists_property, resp_entry.exists ? Qtrue : Qfalse);
      rb_hash_aset(entry, path_property, cb_str_new(resp_entry.path));
      if (!resp_entry.value.empty()) {
        rb_hash_aset(entry, value_property, cb_str_new(resp_entry.value));
      }
      if (resp_entry.ec) {
        rb_hash_aset(
          entry,
          error_property,
          cb_map_error_code(resp_entry.ec,
                            fmt::format("error getting result for spec at index {}, path \"{}\"",
                                        i,
                                        resp_entry.path)));
      }

      rb_ary_store(fields, static_cast<long>(i), entry);
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
cb_Backend_document_lookup_in_any_replica(VALUE self,
                                          VALUE bucket,
                                          VALUE scope,
                                          VALUE collection,
                                          VALUE id,
                                          VALUE specs,
                                          VALUE options,
                                          VALUE observability_handler)
{

  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(specs, T_ARRAY);
  if (RARRAY_LEN(specs) <= 0) {
    rb_raise(rb_eArgError, "Array with specs cannot be empty");
    return Qnil;
  }
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::lookup_in_any_replica_request req{ doc_id };
    cb_extract_timeout(req, options);
    cb_extract_read_preference(req, options);

    static VALUE xattr_property = rb_id2sym(rb_intern("xattr"));
    static VALUE path_property = rb_id2sym(rb_intern("path"));
    static VALUE opcode_property = rb_id2sym(rb_intern("opcode"));

    auto entries_size = static_cast<std::size_t>(RARRAY_LEN(specs));
    for (std::size_t i = 0; i < entries_size; ++i) {
      VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
      cb_check_type(entry, T_HASH);
      VALUE operation = rb_hash_aref(entry, opcode_property);
      cb_check_type(operation, T_SYMBOL);
      bool xattr = RTEST(rb_hash_aref(entry, xattr_property));
      VALUE path = rb_hash_aref(entry, path_property);
      cb_check_type(path, T_STRING);
      auto opcode = core::impl::subdoc::opcode{};
      if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("get_doc")) {
        opcode = core::impl::subdoc::opcode::get_doc;
      } else if (operation_id == rb_intern("get")) {
        opcode = core::impl::subdoc::opcode::get;
      } else if (operation_id == rb_intern("exists")) {
        opcode = core::impl::subdoc::opcode::exists;
      } else if (operation_id == rb_intern("count")) {
        opcode = core::impl::subdoc::opcode::get_count;
      } else {
        throw ruby_exception(
          exc_invalid_argument(),
          rb_sprintf("unsupported operation for subdocument lookup: %+" PRIsVALUE, operation));
      }
      cb_check_type(path, T_STRING);
      req.specs.emplace_back(core::impl::subdoc::command{
        opcode,
        cb_string_new(path),
        {},
        core::impl::subdoc::build_lookup_in_path_flags(xattr, false) });
    }

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::lookup_in_any_replica_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to perform lookup_in_any_replica operation");
    }

    static VALUE deleted_property = rb_id2sym(rb_intern("deleted"));
    static VALUE fields_property = rb_id2sym(rb_intern("fields"));
    static VALUE index_property = rb_id2sym(rb_intern("index"));
    static VALUE exists_property = rb_id2sym(rb_intern("exists"));
    static VALUE cas_property = rb_id2sym(rb_intern("cas"));
    static VALUE value_property = rb_id2sym(rb_intern("value"));
    static VALUE error_property = rb_id2sym(rb_intern("error"));
    static VALUE is_replica_property = rb_id2sym(rb_intern("is_replica"));

    VALUE res = rb_hash_new();
    rb_hash_aset(res, cas_property, cb_cas_to_num(resp.cas));
    VALUE fields = rb_ary_new_capa(static_cast<long>(entries_size));
    rb_hash_aset(res, fields_property, fields);
    rb_hash_aset(res, deleted_property, resp.deleted ? Qtrue : Qfalse);
    rb_hash_aset(res, is_replica_property, resp.is_replica ? Qtrue : Qfalse);

    for (std::size_t i = 0; i < entries_size; ++i) {
      auto resp_entry = resp.fields.at(i);
      VALUE entry = rb_hash_new();
      rb_hash_aset(entry, index_property, ULL2NUM(resp_entry.original_index));
      rb_hash_aset(entry, exists_property, resp_entry.exists ? Qtrue : Qfalse);
      rb_hash_aset(entry, path_property, cb_str_new(resp_entry.path));
      if (!resp_entry.value.empty()) {
        rb_hash_aset(entry, value_property, cb_str_new(resp_entry.value));
      }
      if (resp_entry.ec) {
        rb_hash_aset(
          entry,
          error_property,
          cb_map_error_code(resp_entry.ec,
                            fmt::format("error getting result for spec at index {}, path \"{}\"",
                                        i,
                                        resp_entry.path)));
      }
      rb_ary_store(fields, static_cast<long>(i), entry);
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
cb_Backend_document_lookup_in_all_replicas(VALUE self,
                                           VALUE bucket,
                                           VALUE scope,
                                           VALUE collection,
                                           VALUE id,
                                           VALUE specs,
                                           VALUE options,
                                           VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);
  Check_Type(specs, T_ARRAY);
  if (RARRAY_LEN(specs) <= 0) {
    rb_raise(rb_eArgError, "Array with specs cannot be empty");
    return Qnil;
  }
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::document_id doc_id{
      cb_string_new(bucket),
      cb_string_new(scope),
      cb_string_new(collection),
      cb_string_new(id),
    };

    core::operations::lookup_in_all_replicas_request req{ doc_id };
    cb_extract_timeout(req, options);
    cb_extract_read_preference(req, options);

    static VALUE xattr_property = rb_id2sym(rb_intern("xattr"));
    static VALUE path_property = rb_id2sym(rb_intern("path"));
    static VALUE opcode_property = rb_id2sym(rb_intern("opcode"));

    auto entries_size = static_cast<std::size_t>(RARRAY_LEN(specs));
    for (std::size_t i = 0; i < entries_size; ++i) {
      VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
      cb_check_type(entry, T_HASH);
      VALUE operation = rb_hash_aref(entry, opcode_property);
      cb_check_type(operation, T_SYMBOL);
      bool xattr = RTEST(rb_hash_aref(entry, xattr_property));
      VALUE path = rb_hash_aref(entry, path_property);
      cb_check_type(path, T_STRING);

      auto opcode = core::impl::subdoc::opcode{};
      if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("get_doc")) {
        opcode = core::impl::subdoc::opcode::get_doc;
      } else if (operation_id == rb_intern("get")) {
        opcode = core::impl::subdoc::opcode::get;
      } else if (operation_id == rb_intern("exists")) {
        opcode = core::impl::subdoc::opcode::exists;
      } else if (operation_id == rb_intern("count")) {
        opcode = core::impl::subdoc::opcode::get_count;
      } else {
        throw ruby_exception(
          exc_invalid_argument(),
          rb_sprintf("unsupported operation for subdocument lookup: %+" PRIsVALUE, operation));
      }
      cb_check_type(path, T_STRING);

      req.specs.emplace_back(core::impl::subdoc::command{
        opcode,
        cb_string_new(path),
        {},
        core::impl::subdoc::build_lookup_in_path_flags(xattr, false) });
    }

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::lookup_in_all_replicas_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to perform lookup_in_all_replicas operation");
    }

    static VALUE deleted_property = rb_id2sym(rb_intern("deleted"));
    static VALUE fields_property = rb_id2sym(rb_intern("fields"));
    static VALUE index_property = rb_id2sym(rb_intern("index"));
    static VALUE exists_property = rb_id2sym(rb_intern("exists"));
    static VALUE cas_property = rb_id2sym(rb_intern("cas"));
    static VALUE value_property = rb_id2sym(rb_intern("value"));
    static VALUE error_property = rb_id2sym(rb_intern("error"));
    static VALUE is_replica_property = rb_id2sym(rb_intern("is_replica"));

    auto lookup_in_entries_size = resp.entries.size();
    VALUE res = rb_ary_new_capa(static_cast<long>(lookup_in_entries_size));
    for (std::size_t j = 0; j < lookup_in_entries_size; ++j) {
      auto lookup_in_entry = resp.entries.at(j);
      VALUE lookup_in_entry_res = rb_hash_new();
      rb_hash_aset(lookup_in_entry_res, cas_property, cb_cas_to_num(lookup_in_entry.cas));
      VALUE fields = rb_ary_new_capa(static_cast<long>(entries_size));
      rb_hash_aset(lookup_in_entry_res, fields_property, fields);
      rb_hash_aset(lookup_in_entry_res, deleted_property, lookup_in_entry.deleted ? Qtrue : Qfalse);

      rb_hash_aset(
        lookup_in_entry_res, is_replica_property, lookup_in_entry.is_replica ? Qtrue : Qfalse);

      for (std::size_t i = 0; i < entries_size; ++i) {
        auto field_entry = lookup_in_entry.fields.at(i);
        VALUE entry = rb_hash_new();
        rb_hash_aset(entry, index_property, ULL2NUM(field_entry.original_index));
        rb_hash_aset(entry, exists_property, field_entry.exists ? Qtrue : Qfalse);
        rb_hash_aset(entry, path_property, cb_str_new(field_entry.path));
        if (!field_entry.value.empty()) {
          rb_hash_aset(entry, value_property, cb_str_new(field_entry.value));
        }
        if (field_entry.ec) {
          rb_hash_aset(
            entry,
            error_property,
            cb_map_error_code(field_entry.ec,
                              fmt::format("error getting result for spec at index {}, path \"{}\"",
                                          i,
                                          field_entry.path)));
        }

        rb_ary_store(fields, static_cast<long>(i), entry);
      }
      rb_ary_store(res, static_cast<long>(j), lookup_in_entry_res);
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
cb_Backend_document_mutate_in(VALUE self,
                              VALUE bucket,
                              VALUE scope,
                              VALUE collection,
                              VALUE id,
                              VALUE specs,
                              VALUE options,
                              VALUE observability_handler)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(id, T_STRING);

  Check_Type(specs, T_ARRAY);
  if (RARRAY_LEN(specs) <= 0) {
    rb_raise(rb_eArgError, "Array with specs cannot be empty");
    return Qnil;
  }
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::mutate_in_request req{
      core::document_id{
        cb_string_new(bucket),
        cb_string_new(scope),
        cb_string_new(collection),
        cb_string_new(id),
      },
    };
    cb_extract_timeout(req, options);
    cb_extract_durability_level(req, options);
    cb_extract_expiry(req, options);
    cb_extract_preserve_expiry(req, options);
    cb_extract_option_bool(req.access_deleted, options, "access_deleted");
    cb_extract_option_bool(req.create_as_deleted, options, "create_as_deleted");
    cb_extract_cas(req, options);
    cb_extract_store_semantics(req, options);

    static VALUE xattr_property = rb_id2sym(rb_intern("xattr"));
    static VALUE create_path_property = rb_id2sym(rb_intern("create_path"));
    static VALUE expand_macros_property = rb_id2sym(rb_intern("expand_macros"));
    static VALUE path_property = rb_id2sym(rb_intern("path"));
    static VALUE opcode_property = rb_id2sym(rb_intern("opcode"));
    static VALUE param_property = rb_id2sym(rb_intern("param"));

    couchbase::mutate_in_specs cxx_specs;
    {
      auto entries_size = static_cast<std::size_t>(RARRAY_LEN(specs));
      for (std::size_t i = 0; i < entries_size; ++i) {
        VALUE entry = rb_ary_entry(specs, static_cast<long>(i));
        cb_check_type(entry, T_HASH);
        bool xattr = RTEST(rb_hash_aref(entry, xattr_property));
        bool create_path = RTEST(rb_hash_aref(entry, create_path_property));
        bool expand_macros = RTEST(rb_hash_aref(entry, expand_macros_property));
        VALUE path = rb_hash_aref(entry, path_property);
        cb_check_type(path, T_STRING);
        VALUE operation = rb_hash_aref(entry, opcode_property);
        cb_check_type(operation, T_SYMBOL);
        VALUE param = rb_hash_aref(entry, param_property);
        if (ID operation_id = rb_sym2id(operation); operation_id == rb_intern("dict_add")) {
          cb_check_type(param, T_STRING);
          cxx_specs.push_back(couchbase::mutate_in_specs::insert_raw(
                                cb_string_new(path), cb_binary_new(param), expand_macros)
                                .xattr(xattr)
                                .create_path(create_path));
        } else if (operation_id == rb_intern("dict_upsert")) {
          cb_check_type(param, T_STRING);

          cxx_specs.push_back(couchbase::mutate_in_specs::upsert_raw(
                                cb_string_new(path), cb_binary_new(param), expand_macros)
                                .xattr(xattr)
                                .create_path(create_path));
        } else if (operation_id == rb_intern("remove")) {
          cxx_specs.push_back(couchbase::mutate_in_specs::remove(cb_string_new(path)).xattr(xattr));
        } else if (operation_id == rb_intern("replace")) {
          cb_check_type(param, T_STRING);
          cxx_specs.push_back(couchbase::mutate_in_specs::replace_raw(
                                cb_string_new(path), cb_binary_new(param), expand_macros)
                                .xattr(xattr));
        } else if (operation_id == rb_intern("array_push_last")) {
          cb_check_type(param, T_STRING);
          cxx_specs.push_back(
            couchbase::mutate_in_specs::array_append_raw(cb_string_new(path), cb_binary_new(param))
              .xattr(xattr)
              .create_path(create_path));
        } else if (operation_id == rb_intern("array_push_first")) {
          cb_check_type(param, T_STRING);
          cxx_specs.push_back(
            couchbase::mutate_in_specs::array_prepend_raw(cb_string_new(path), cb_binary_new(param))
              .xattr(xattr)
              .create_path(create_path));
        } else if (operation_id == rb_intern("array_insert")) {
          cb_check_type(param, T_STRING);
          cxx_specs.push_back(
            couchbase::mutate_in_specs::array_insert_raw(cb_string_new(path), cb_binary_new(param))
              .xattr(xattr)
              .create_path(create_path));
        } else if (operation_id == rb_intern("array_add_unique")) {
          cb_check_type(param, T_STRING);
          cxx_specs.push_back(couchbase::mutate_in_specs::array_add_unique_raw(
                                cb_string_new(path), cb_binary_new(param), expand_macros)
                                .xattr(xattr)
                                .create_path(create_path));
        } else if (operation_id == rb_intern("counter")) {
          if (TYPE(param) == T_FIXNUM || TYPE(param) == T_BIGNUM) {
            if (std::int64_t num = NUM2LL(param); num < 0) {
              cxx_specs.push_back(
                couchbase::mutate_in_specs::decrement(cb_string_new(path), -1 * num)
                  .xattr(xattr)
                  .create_path(create_path));
            } else {
              cxx_specs.push_back(couchbase::mutate_in_specs::increment(cb_string_new(path), num)
                                    .xattr(xattr)
                                    .create_path(create_path));
            }
          } else {
            throw ruby_exception(
              exc_invalid_argument(),
              rb_sprintf("subdocument counter operation expects number, but given: %+" PRIsVALUE,
                         param));
          }
        } else if (operation_id == rb_intern("set_doc")) {
          cb_check_type(param, T_STRING);
          cxx_specs.push_back(
            couchbase::mutate_in_specs::replace_raw("", cb_binary_new(param), expand_macros)
              .xattr(xattr));
        } else if (operation_id == rb_intern("remove_doc")) {
          cxx_specs.push_back(couchbase::mutate_in_specs::remove("").xattr(xattr));
        } else {
          throw ruby_exception(
            exc_invalid_argument(),
            rb_sprintf("unsupported operation for subdocument mutation: %+" PRIsVALUE, operation));
        }
      }
    }
    req.specs = cxx_specs.specs();

    auto parent_span = cb_create_parent_span(req, self);

    std::promise<core::operations::mutate_in_response> promise;
    auto f = promise.get_future();

    if (const auto legacy_durability = extract_legacy_durability_constraints(options);
        legacy_durability.has_value()) {
      cluster.execute(
        core::operations::mutate_in_request_with_legacy_durability{
          std::move(req),
          legacy_durability.value().first,
          legacy_durability.value().second,
        },
        [promise = std::move(promise)](auto&& resp) mutable {
          promise.set_value(std::forward<decltype(resp)>(resp));
        });
    } else {
      cluster.execute(std::move(req), [promise = std::move(promise)](auto&& resp) mutable {
        promise.set_value(std::forward<decltype(resp)>(resp));
      });
    }

    auto resp = cb_wait_for_future(f);
    cb_add_core_spans(observability_handler, std::move(parent_span), resp.ctx.retry_attempts());
    if (resp.ctx.ec()) {
      cb_throw_error(resp.ctx, "unable to mutate_in");
    }

    static VALUE deleted_property = rb_id2sym(rb_intern("deleted"));
    static VALUE fields_property = rb_id2sym(rb_intern("fields"));
    static VALUE index_property = rb_id2sym(rb_intern("index"));
    static VALUE value_property = rb_id2sym(rb_intern("value"));

    VALUE res = cb_create_mutation_result(resp);
    rb_hash_aset(res, deleted_property, resp.deleted ? Qtrue : Qfalse);
    VALUE fields = rb_ary_new_capa(static_cast<long>(resp.fields.size()));
    rb_hash_aset(res, fields_property, fields);
    for (std::size_t i = 0; i < resp.fields.size(); ++i) {
      VALUE entry = rb_hash_new();
      rb_hash_aset(entry, index_property, ULL2NUM(i));
      rb_hash_aset(entry,
                   path_property,
                   rb_hash_aref(rb_ary_entry(specs, static_cast<long>(i)), path_property));
      if (!resp.fields.at(i).value.empty()) {
        rb_hash_aset(entry, value_property, cb_str_new(resp.fields.at(i).value));
      }
      rb_ary_store(fields, static_cast<long>(i), entry);
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
init_crud(VALUE cBackend)
{
  rb_define_method(cBackend, "document_get", cb_Backend_document_get, 6);
  rb_define_method(cBackend, "document_get_any_replica", cb_Backend_document_get_any_replica, 6);
  rb_define_method(cBackend, "document_get_all_replicas", cb_Backend_document_get_all_replicas, 6);
  rb_define_method(cBackend, "document_get_projected", cb_Backend_document_get_projected, 6);
  rb_define_method(cBackend, "document_get_and_lock", cb_Backend_document_get_and_lock, 7);
  rb_define_method(cBackend, "document_get_and_touch", cb_Backend_document_get_and_touch, 7);
  rb_define_method(cBackend, "document_insert", cb_Backend_document_insert, 8);
  rb_define_method(cBackend, "document_replace", cb_Backend_document_replace, 8);
  rb_define_method(cBackend, "document_upsert", cb_Backend_document_upsert, 8);
  rb_define_method(cBackend, "document_append", cb_Backend_document_append, 7);
  rb_define_method(cBackend, "document_prepend", cb_Backend_document_prepend, 7);
  rb_define_method(cBackend, "document_remove", cb_Backend_document_remove, 6);
  rb_define_method(cBackend, "document_lookup_in", cb_Backend_document_lookup_in, 7);
  rb_define_method(
    cBackend, "document_lookup_in_any_replica", cb_Backend_document_lookup_in_any_replica, 7);
  rb_define_method(
    cBackend, "document_lookup_in_all_replicas", cb_Backend_document_lookup_in_all_replicas, 7);
  rb_define_method(cBackend, "document_mutate_in", cb_Backend_document_mutate_in, 7);
  rb_define_method(cBackend, "document_touch", cb_Backend_document_touch, 7);
  rb_define_method(cBackend, "document_exists", cb_Backend_document_exists, 6);
  rb_define_method(cBackend, "document_unlock", cb_Backend_document_unlock, 7);
  rb_define_method(cBackend, "document_increment", cb_Backend_document_increment, 6);
  rb_define_method(cBackend, "document_decrement", cb_Backend_document_decrement, 6);
}
} // namespace couchbase::ruby
