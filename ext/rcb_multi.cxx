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
#include <core/operations/document_get.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/upsert_options.hxx>

#include <future>
#include <memory>

#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{

void
cb_extract_array_of_ids(std::vector<core::document_id>& ids, VALUE arg)
{
  if (TYPE(arg) != T_ARRAY) {
    throw ruby_exception(
      rb_eArgError,
      rb_sprintf("Type of IDs argument must be an Array, but given %+" PRIsVALUE, arg));
  }
  auto num_of_ids = static_cast<std::size_t>(RARRAY_LEN(arg));
  if (num_of_ids < 1) {
    throw ruby_exception(rb_eArgError, "Array of IDs must not be empty");
  }
  ids.reserve(num_of_ids);
  for (std::size_t i = 0; i < num_of_ids; ++i) {
    VALUE entry = rb_ary_entry(arg, static_cast<long>(i));
    if (TYPE(entry) != T_ARRAY || RARRAY_LEN(entry) != 4) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("ID tuple must be represented as an Array[bucket, scope, "
                                      "collection, id], but given %+" PRIsVALUE,
                                      entry));
    }
    VALUE bucket = rb_ary_entry(entry, 0);
    if (TYPE(bucket) != T_STRING) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("Bucket must be a String, but given %+" PRIsVALUE, bucket));
    }
    VALUE scope = rb_ary_entry(entry, 1);
    if (TYPE(scope) != T_STRING) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("Scope must be a String, but given %+" PRIsVALUE, scope));
    }
    VALUE collection = rb_ary_entry(entry, 2);
    if (TYPE(collection) != T_STRING) {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("Collection must be a String, but given %+" PRIsVALUE, collection));
    }
    VALUE id = rb_ary_entry(entry, 3);
    if (TYPE(id) != T_STRING) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("ID must be a String, but given %+" PRIsVALUE, id));
    }
    ids.emplace_back(
      cb_string_new(bucket), cb_string_new(scope), cb_string_new(collection), cb_string_new(id));
  }
}

void
cb_extract_array_of_id_content(
  std::vector<std::pair<std::string, couchbase::codec::encoded_value>>& id_content,
  VALUE arg)
{
  if (TYPE(arg) != T_ARRAY) {
    throw ruby_exception(
      rb_eArgError,
      rb_sprintf("Type of ID/content tuples must be an Array, but given %+" PRIsVALUE, arg));
  }
  auto num_of_tuples = static_cast<std::size_t>(RARRAY_LEN(arg));
  if (num_of_tuples < 1) {
    throw ruby_exception(rb_eArgError, "Array of ID/content tuples must not be empty");
  }
  id_content.reserve(num_of_tuples);
  for (std::size_t i = 0; i < num_of_tuples; ++i) {
    VALUE entry = rb_ary_entry(arg, static_cast<long>(i));
    if (TYPE(entry) != T_ARRAY || RARRAY_LEN(entry) != 3) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("ID/content tuple must be represented as an Array[id, "
                                      "content, flags], but given %+" PRIsVALUE,
                                      entry));
    }
    VALUE id = rb_ary_entry(entry, 0);
    if (TYPE(id) != T_STRING) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("ID must be a String, but given %+" PRIsVALUE, id));
    }
    VALUE content = rb_ary_entry(entry, 1);
    if (TYPE(content) != T_STRING) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("Content must be a String, but given %+" PRIsVALUE, content));
    }
    VALUE flags = rb_ary_entry(entry, 2);
    if (TYPE(flags) != T_FIXNUM) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("Flags must be an Integer, but given %+" PRIsVALUE, flags));
    }
    id_content.emplace_back(
      cb_string_new(id),
      couchbase::codec::encoded_value{ cb_binary_new(content), FIX2UINT(flags) });
  }
}

void
cb_extract_array_of_id_cas(std::vector<std::pair<std::string, couchbase::cas>>& id_cas, VALUE arg)
{
  if (TYPE(arg) != T_ARRAY) {
    throw ruby_exception(
      rb_eArgError,
      rb_sprintf("Type of ID/CAS tuples must be an Array, but given %+" PRIsVALUE, arg));
  }
  auto num_of_tuples = static_cast<std::size_t>(RARRAY_LEN(arg));
  if (num_of_tuples < 1) {
    rb_raise(rb_eArgError, "Array of ID/CAS tuples must not be empty");
  }
  id_cas.reserve(num_of_tuples);
  for (std::size_t i = 0; i < num_of_tuples; ++i) {
    VALUE entry = rb_ary_entry(arg, static_cast<long>(i));
    if (TYPE(entry) != T_ARRAY || RARRAY_LEN(entry) != 2) {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf(
          "ID/content tuple must be represented as an Array[id, CAS], but given %+" PRIsVALUE,
          entry));
    }
    VALUE id = rb_ary_entry(entry, 0);
    if (TYPE(id) != T_STRING) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("ID must be a String, but given %+" PRIsVALUE, id));
    }
    couchbase::cas cas_val{};
    if (VALUE cas = rb_ary_entry(entry, 1); !NIL_P(cas)) {
      cb_extract_cas(cas_val, cas);
    }

    id_cas.emplace_back(cb_string_new(id), cas_val);
  }
}

VALUE
cb_Backend_document_get_multi(VALUE self, VALUE keys, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  try {
    std::chrono::milliseconds timeout{ 0 };
    cb_extract_timeout(timeout, options);

    std::vector<core::document_id> ids{};
    cb_extract_array_of_ids(ids, keys);

    auto num_of_ids = ids.size();
    std::vector<std::shared_ptr<std::promise<core::operations::get_response>>> promises;
    promises.reserve(num_of_ids);

    for (auto& id : ids) {
      core::operations::get_request req{ std::move(id) };
      if (timeout.count() > 0) {
        req.timeout = timeout;
      }
      auto promise = std::make_shared<std::promise<core::operations::get_response>>();
      cluster->execute(req, [promise](auto&& resp) {
        promise->set_value(std::forward<decltype(resp)>(resp));
      });
      promises.emplace_back(promise);
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(num_of_ids));
    for (const auto& promise : promises) {
      auto resp = promise->get_future().get();
      VALUE entry = rb_hash_new();
      if (resp.ctx.ec()) {
        rb_hash_aset(entry,
                     rb_id2sym(rb_intern("error")),
                     cb_map_error(resp.ctx, "unable to (multi)fetch document"));
      }
      rb_hash_aset(entry, rb_id2sym(rb_intern("id")), cb_str_new(resp.ctx.id()));
      rb_hash_aset(entry, rb_id2sym(rb_intern("content")), cb_str_new(resp.value));
      rb_hash_aset(entry, rb_id2sym(rb_intern("cas")), cb_cas_to_num(resp.cas));
      rb_hash_aset(entry, rb_id2sym(rb_intern("flags")), UINT2NUM(resp.flags));
      rb_ary_push(res, entry);
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
cb_Backend_document_upsert_multi(VALUE self,
                                 VALUE bucket,
                                 VALUE scope,
                                 VALUE collection,
                                 VALUE id_content,
                                 VALUE options)
{
  const auto& core = cb_backend_to_cluster(self);

  try {
    couchbase::upsert_options opts;
    set_timeout(opts, options);
    set_expiry(opts, options);
    set_durability(opts, options);
    set_preserve_expiry(opts, options);

    auto c = couchbase::cluster(*core)
               .bucket(cb_string_new(bucket))
               .scope(cb_string_new(scope))
               .collection(cb_string_new(collection));

    std::vector<std::pair<std::string, couchbase::codec::encoded_value>> tuples{};
    cb_extract_array_of_id_content(tuples, id_content);

    auto num_of_tuples = tuples.size();
    std::vector<
      std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>>
      futures;
    futures.reserve(num_of_tuples);

    for (auto& [id, content] : tuples) {
      futures.emplace_back(c.upsert(std::move(id), content, opts));
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(num_of_tuples));
    for (auto& f : futures) {
      auto [ctx, resp] = f.get();
      VALUE entry = to_mutation_result_value(resp);
      if (ctx.ec()) {
        rb_hash_aset(
          entry, rb_id2sym(rb_intern("error")), cb_map_error(ctx, "unable (multi)upsert"));
      }
      rb_hash_aset(entry, rb_id2sym(rb_intern("id")), cb_str_new(ctx.id()));
      rb_ary_push(res, entry);
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
cb_Backend_document_remove_multi(VALUE self,
                                 VALUE bucket,
                                 VALUE scope,
                                 VALUE collection,
                                 VALUE id_cas,
                                 VALUE options)
{
  const auto& core = cb_backend_to_cluster(self);

  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    couchbase::remove_options opts;
    set_timeout(opts, options);
    set_durability(opts, options);

    std::vector<std::pair<std::string, couchbase::cas>> tuples{};
    cb_extract_array_of_id_cas(tuples, id_cas);

    auto c = couchbase::cluster(*core)
               .bucket(cb_string_new(bucket))
               .scope(cb_string_new(scope))
               .collection(cb_string_new(collection));

    auto num_of_tuples = tuples.size();
    std::vector<
      std::future<std::pair<couchbase::key_value_error_context, couchbase::mutation_result>>>
      futures;
    futures.reserve(num_of_tuples);

    for (auto& [id, cas] : tuples) {
      opts.cas(cas);
      futures.emplace_back(c.remove(std::move(id), opts));
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(num_of_tuples));
    for (auto& f : futures) {
      auto [ctx, resp] = f.get();
      VALUE entry = to_mutation_result_value(resp);
      if (ctx.ec()) {
        rb_hash_aset(
          entry, rb_id2sym(rb_intern("error")), cb_map_error(ctx, "unable (multi)remove"));
      }
      rb_hash_aset(entry, rb_id2sym(rb_intern("id")), cb_str_new(ctx.id()));
      rb_ary_push(res, entry);
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
init_multi(VALUE cBackend)
{
  rb_define_method(cBackend, "document_get_multi", cb_Backend_document_get_multi, 2);
  rb_define_method(cBackend, "document_remove_multi", cb_Backend_document_remove_multi, 5);
  rb_define_method(cBackend, "document_upsert_multi", cb_Backend_document_upsert_multi, 5);
}
} // namespace couchbase::ruby
