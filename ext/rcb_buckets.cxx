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
#include <core/management/bucket_settings.hxx>
#include <core/operations/management/bucket_create.hxx>
#include <core/operations/management/bucket_drop.hxx>
#include <core/operations/management/bucket_flush.hxx>
#include <core/operations/management/bucket_get.hxx>
#include <core/operations/management/bucket_get_all.hxx>
#include <core/operations/management/bucket_update.hxx>

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
void
cb_generate_bucket_settings(VALUE bucket,
                            core::management::cluster::bucket_settings& entry,
                            bool is_create)
{
  if (VALUE bucket_type = rb_hash_aref(bucket, rb_id2sym(rb_intern("bucket_type")));
      !NIL_P(bucket_type)) {
    if (TYPE(bucket_type) == T_SYMBOL) {
      if (bucket_type == rb_id2sym(rb_intern("couchbase")) ||
          bucket_type == rb_id2sym(rb_intern("membase"))) {
        entry.bucket_type = core::management::cluster::bucket_type::couchbase;
      } else if (bucket_type == rb_id2sym(rb_intern("memcached"))) {
        entry.bucket_type = core::management::cluster::bucket_type::memcached;
      } else if (bucket_type == rb_id2sym(rb_intern("ephemeral"))) {
        entry.bucket_type = core::management::cluster::bucket_type::ephemeral;
      } else {
        throw ruby_exception(rb_eArgError,
                             rb_sprintf("unknown bucket type, given %+" PRIsVALUE, bucket_type));
      }
    } else {
      throw ruby_exception(
        rb_eArgError, rb_sprintf("bucket type must be a Symbol, given %+" PRIsVALUE, bucket_type));
    }
  }

  if (VALUE name = rb_hash_aref(bucket, rb_id2sym(rb_intern("name"))); TYPE(name) == T_STRING) {
    entry.name = cb_string_new(name);
  } else {
    throw ruby_exception(rb_eArgError,
                         rb_sprintf("bucket name must be a String, given %+" PRIsVALUE, name));
  }

  if (VALUE quota = rb_hash_aref(bucket, rb_id2sym(rb_intern("ram_quota_mb"))); !NIL_P(quota)) {
    if (TYPE(quota) == T_FIXNUM) {
      entry.ram_quota_mb = FIX2ULONG(quota);
    } else {
      throw ruby_exception(
        rb_eArgError, rb_sprintf("bucket RAM quota must be an Integer, given %+" PRIsVALUE, quota));
    }
  }

  if (VALUE expiry = rb_hash_aref(bucket, rb_id2sym(rb_intern("max_expiry"))); !NIL_P(expiry)) {
    if (TYPE(expiry) == T_FIXNUM) {
      entry.max_expiry = FIX2UINT(expiry);
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("bucket max expiry must be an Integer, given %+" PRIsVALUE, expiry));
    }
  }

  if (VALUE num_replicas = rb_hash_aref(bucket, rb_id2sym(rb_intern("num_replicas")));
      !NIL_P(num_replicas)) {
    if (TYPE(num_replicas) == T_FIXNUM) {
      entry.num_replicas = FIX2UINT(num_replicas);
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("bucket number of replicas must be an Integer, given %+" PRIsVALUE,
                   num_replicas));
    }
  }

  if (VALUE replica_indexes = rb_hash_aref(bucket, rb_id2sym(rb_intern("replica_indexes")));
      !NIL_P(replica_indexes)) {
    entry.replica_indexes = RTEST(replica_indexes);
  }

  if (VALUE flush_enabled = rb_hash_aref(bucket, rb_id2sym(rb_intern("flush_enabled")));
      !NIL_P(flush_enabled)) {
    entry.flush_enabled = RTEST(flush_enabled);
  }

  if (VALUE compression_mode = rb_hash_aref(bucket, rb_id2sym(rb_intern("compression_mode")));
      !NIL_P(compression_mode)) {
    if (TYPE(compression_mode) == T_SYMBOL) {
      if (compression_mode == rb_id2sym(rb_intern("active"))) {
        entry.compression_mode = core::management::cluster::bucket_compression::active;
      } else if (compression_mode == rb_id2sym(rb_intern("passive"))) {
        entry.compression_mode = core::management::cluster::bucket_compression::passive;
      } else if (compression_mode == rb_id2sym(rb_intern("off"))) {
        entry.compression_mode = core::management::cluster::bucket_compression::off;
      } else {
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("unknown compression mode, given %+" PRIsVALUE, compression_mode));
      }
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("bucket compression mode must be a Symbol, given %+" PRIsVALUE,
                   compression_mode));
    }
  }

  if (VALUE eviction_policy = rb_hash_aref(bucket, rb_id2sym(rb_intern("eviction_policy")));
      !NIL_P(eviction_policy)) {
    if (TYPE(eviction_policy) == T_SYMBOL) {
      if (eviction_policy == rb_id2sym(rb_intern("full"))) {
        entry.eviction_policy = core::management::cluster::bucket_eviction_policy::full;
      } else if (eviction_policy == rb_id2sym(rb_intern("value_only"))) {
        entry.eviction_policy = core::management::cluster::bucket_eviction_policy::value_only;
      } else if (eviction_policy == rb_id2sym(rb_intern("no_eviction"))) {
        entry.eviction_policy = core::management::cluster::bucket_eviction_policy::no_eviction;
      } else if (eviction_policy == rb_id2sym(rb_intern("not_recently_used"))) {
        entry.eviction_policy =
          core::management::cluster::bucket_eviction_policy::not_recently_used;
      } else {
        throw ruby_exception(
          rb_eArgError, rb_sprintf("unknown eviction policy, given %+" PRIsVALUE, eviction_policy));
      }
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("bucket eviction policy must be a Symbol, given %+" PRIsVALUE, eviction_policy));
    }
  }

  if (VALUE storage_backend = rb_hash_aref(bucket, rb_id2sym(rb_intern("storage_backend")));
      !NIL_P(storage_backend)) {
    if (TYPE(storage_backend) == T_SYMBOL) {
      if (storage_backend == rb_id2sym(rb_intern("couchstore"))) {
        entry.storage_backend = core::management::cluster::bucket_storage_backend::couchstore;
      } else if (storage_backend == rb_id2sym(rb_intern("magma"))) {
        entry.storage_backend = core::management::cluster::bucket_storage_backend::magma;
      } else {
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("unknown storage backend type, given %+" PRIsVALUE, storage_backend));
      }
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("bucket storage backend type must be a Symbol, given %+" PRIsVALUE,
                   storage_backend));
    }
  }

  if (VALUE minimum_level = rb_hash_aref(bucket, rb_id2sym(rb_intern("minimum_durability_level")));
      !NIL_P(minimum_level)) {
    if (TYPE(minimum_level) == T_SYMBOL) {
      if (minimum_level == rb_id2sym(rb_intern("none"))) {
        entry.minimum_durability_level = couchbase::durability_level::none;
      } else if (minimum_level == rb_id2sym(rb_intern("majority"))) {
        entry.minimum_durability_level = couchbase::durability_level::majority;
      } else if (minimum_level == rb_id2sym(rb_intern("majority_and_persist_to_active"))) {
        entry.minimum_durability_level =
          couchbase::durability_level::majority_and_persist_to_active;
      } else if (minimum_level == rb_id2sym(rb_intern("persist_to_majority"))) {
        entry.minimum_durability_level = couchbase::durability_level::persist_to_majority;
      } else {
        throw ruby_exception(
          rb_eArgError, rb_sprintf("unknown durability level, given %+" PRIsVALUE, minimum_level));
      }
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("bucket minimum durability level must be a Symbol, given %+" PRIsVALUE,
                   minimum_level));
    }
  }

  if (VALUE history_retention_collection_default =
        rb_hash_aref(bucket, rb_id2sym(rb_intern("history_retention_collection_default")));
      !NIL_P(history_retention_collection_default)) {
    entry.history_retention_collection_default = RTEST(history_retention_collection_default);
  }

  if (VALUE history_retention_bytes =
        rb_hash_aref(bucket, rb_id2sym(rb_intern("history_retention_bytes")));
      !NIL_P(history_retention_bytes)) {
    if (TYPE(history_retention_bytes) == T_FIXNUM) {
      entry.history_retention_bytes = FIX2UINT(history_retention_bytes);
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("history retention bytes must be an Integer, given %+" PRIsVALUE,
                   history_retention_bytes));
    }
  }

  if (VALUE history_retention_duration =
        rb_hash_aref(bucket, rb_id2sym(rb_intern("history_retention_duration")));
      !NIL_P(history_retention_duration)) {
    if (TYPE(history_retention_duration) == T_FIXNUM) {
      entry.history_retention_duration = FIX2UINT(history_retention_duration);
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("history retention duration must be an Integer, given %+" PRIsVALUE,
                   history_retention_duration));
    }
  }

  if (VALUE num_vbuckets = rb_hash_aref(bucket, rb_id2sym(rb_intern("num_vbuckets")));
      !NIL_P(num_vbuckets)) {
    if (TYPE(num_vbuckets) == T_FIXNUM) {
      entry.num_vbuckets = FIX2UINT(num_vbuckets);
    } else {
      throw ruby_exception(
        rb_eArgError,
        rb_sprintf("num vbuckets must be an Integer, given %+" PRIsVALUE, num_vbuckets));
    }
  }

  if (is_create) {
    if (VALUE conflict_resolution_type =
          rb_hash_aref(bucket, rb_id2sym(rb_intern("conflict_resolution_type")));
        !NIL_P(conflict_resolution_type)) {
      if (TYPE(conflict_resolution_type) == T_SYMBOL) {
        if (conflict_resolution_type == rb_id2sym(rb_intern("timestamp"))) {
          entry.conflict_resolution_type =
            core::management::cluster::bucket_conflict_resolution::timestamp;
        } else if (conflict_resolution_type == rb_id2sym(rb_intern("sequence_number"))) {
          entry.conflict_resolution_type =
            core::management::cluster::bucket_conflict_resolution::sequence_number;
        } else if (conflict_resolution_type == rb_id2sym(rb_intern("custom"))) {
          entry.conflict_resolution_type =
            core::management::cluster::bucket_conflict_resolution::custom;
        } else {
          throw ruby_exception(rb_eArgError,
                               rb_sprintf("unknown conflict resolution type, given %+" PRIsVALUE,
                                          conflict_resolution_type));
        }
      } else {
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("bucket conflict resolution type must be a Symbol, given %+" PRIsVALUE,
                     conflict_resolution_type));
      }
    }
  }
}

VALUE
cb_Backend_bucket_create(VALUE self, VALUE bucket_settings, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_settings, T_HASH);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::bucket_create_request req{};
    cb_extract_timeout(req, options);
    cb_generate_bucket_settings(bucket_settings, req.bucket, true);
    std::promise<core::operations::management::bucket_create_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format("unable to create bucket \"{}\" on the cluster ({})",
                                 req.bucket.name,
                                 resp.error_message));
    }

    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_bucket_update(VALUE self, VALUE bucket_settings, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_settings, T_HASH);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }
  try {
    core::operations::management::bucket_update_request req{};
    cb_extract_timeout(req, options);
    cb_generate_bucket_settings(bucket_settings, req.bucket, false);
    std::promise<core::operations::management::bucket_update_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format("unable to update bucket \"{}\" on the cluster ({})",
                                 req.bucket.name,
                                 resp.error_message));
    }
    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_bucket_drop(VALUE self, VALUE bucket_name, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::bucket_drop_request req{ cb_string_new(bucket_name) };
    cb_extract_timeout(req, options);
    std::promise<core::operations::management::bucket_drop_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format("unable to remove bucket \"{}\" on the cluster", req.name));
    }
    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

VALUE
cb_Backend_bucket_flush(VALUE self, VALUE bucket_name, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::bucket_flush_request req{ cb_string_new(bucket_name) };
    cb_extract_timeout(req, options);
    std::promise<core::operations::management::bucket_flush_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format("unable to flush bucket \"{}\" on the cluster", req.name));
    }

    return Qtrue;
  } catch (const std::system_error& se) {
    rb_exc_raise(cb_map_error_code(
      se.code(), fmt::format("failed to perform {}: {}", __func__, se.what()), false));
  } catch (const ruby_exception& e) {
    rb_exc_raise(e.exception_object());
  }
  return Qnil;
}

void
cb_extract_bucket_settings(const core::management::cluster::bucket_settings& entry, VALUE bucket)
{
  switch (entry.bucket_type) {
    case core::management::cluster::bucket_type::couchbase:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("couchbase")));
      break;
    case core::management::cluster::bucket_type::memcached:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("memcached")));
      break;
    case core::management::cluster::bucket_type::ephemeral:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), rb_id2sym(rb_intern("ephemeral")));
      break;
    case core::management::cluster::bucket_type::unknown:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("bucket_type")), Qnil);
      break;
  }
  rb_hash_aset(bucket, rb_id2sym(rb_intern("name")), cb_str_new(entry.name));
  rb_hash_aset(bucket, rb_id2sym(rb_intern("uuid")), cb_str_new(entry.uuid));
  rb_hash_aset(bucket, rb_id2sym(rb_intern("ram_quota_mb")), ULL2NUM(entry.ram_quota_mb));
  if (const auto& val = entry.max_expiry; val.has_value()) {
    rb_hash_aset(bucket, rb_id2sym(rb_intern("max_expiry")), ULONG2NUM(val.value()));
  }
  switch (entry.compression_mode) {
    case core::management::cluster::bucket_compression::off:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("off")));
      break;
    case core::management::cluster::bucket_compression::active:
      rb_hash_aset(
        bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("active")));
      break;
    case core::management::cluster::bucket_compression::passive:
      rb_hash_aset(
        bucket, rb_id2sym(rb_intern("compression_mode")), rb_id2sym(rb_intern("passive")));
      break;
    case core::management::cluster::bucket_compression::unknown:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("compression_mode")), Qnil);
      break;
  }
  if (const auto& val = entry.num_replicas; val.has_value()) {
    rb_hash_aset(bucket, rb_id2sym(rb_intern("num_replicas")), ULONG2NUM(val.value()));
  }
  if (const auto& val = entry.replica_indexes; val.has_value()) {
    rb_hash_aset(bucket, rb_id2sym(rb_intern("replica_indexes")), val.value() ? Qtrue : Qfalse);
  }
  if (const auto& val = entry.flush_enabled; val.has_value()) {
    rb_hash_aset(bucket, rb_id2sym(rb_intern("flush_enabled")), val.value() ? Qtrue : Qfalse);
  }
  switch (entry.eviction_policy) {
    case core::management::cluster::bucket_eviction_policy::full:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("full")));
      break;
    case core::management::cluster::bucket_eviction_policy::value_only:
      rb_hash_aset(
        bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("value_only")));
      break;
    case core::management::cluster::bucket_eviction_policy::no_eviction:
      rb_hash_aset(
        bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("no_eviction")));
      break;
    case core::management::cluster::bucket_eviction_policy::not_recently_used:
      rb_hash_aset(
        bucket, rb_id2sym(rb_intern("eviction_policy")), rb_id2sym(rb_intern("not_recently_used")));
      break;
    case core::management::cluster::bucket_eviction_policy::unknown:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("eviction_policy")), Qnil);
      break;
  }
  switch (entry.conflict_resolution_type) {
    case core::management::cluster::bucket_conflict_resolution::timestamp:
      rb_hash_aset(bucket,
                   rb_id2sym(rb_intern("conflict_resolution_type")),
                   rb_id2sym(rb_intern("timestamp")));
      break;
    case core::management::cluster::bucket_conflict_resolution::sequence_number:
      rb_hash_aset(bucket,
                   rb_id2sym(rb_intern("conflict_resolution_type")),
                   rb_id2sym(rb_intern("sequence_number")));
      break;
    case core::management::cluster::bucket_conflict_resolution::custom:
      rb_hash_aset(
        bucket, rb_id2sym(rb_intern("conflict_resolution_type")), rb_id2sym(rb_intern("custom")));
      break;
    case core::management::cluster::bucket_conflict_resolution::unknown:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("conflict_resolution_type")), Qnil);
      break;
  }
  if (entry.minimum_durability_level) {
    switch (entry.minimum_durability_level.value()) {
      case couchbase::durability_level::none:
        rb_hash_aset(
          bucket, rb_id2sym(rb_intern("minimum_durability_level")), rb_id2sym(rb_intern("none")));
        break;
      case couchbase::durability_level::majority:
        rb_hash_aset(bucket,
                     rb_id2sym(rb_intern("minimum_durability_level")),
                     rb_id2sym(rb_intern("majority")));
        break;
      case couchbase::durability_level::majority_and_persist_to_active:
        rb_hash_aset(bucket,
                     rb_id2sym(rb_intern("minimum_durability_level")),
                     rb_id2sym(rb_intern("majority_and_persist_to_active")));
        break;
      case couchbase::durability_level::persist_to_majority:
        rb_hash_aset(bucket,
                     rb_id2sym(rb_intern("minimum_durability_level")),
                     rb_id2sym(rb_intern("persist_to_majority")));
        break;
    }
  }
  switch (entry.storage_backend) {
    case core::management::cluster::bucket_storage_backend::couchstore:
      rb_hash_aset(
        bucket, rb_id2sym(rb_intern("storage_backend")), rb_id2sym(rb_intern("couchstore")));
      break;
    case core::management::cluster::bucket_storage_backend::magma:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("storage_backend")), rb_id2sym(rb_intern("magma")));
      break;
    case core::management::cluster::bucket_storage_backend::unknown:
      rb_hash_aset(bucket, rb_id2sym(rb_intern("storage_backend")), Qnil);
      break;
  }
  if (entry.history_retention_collection_default.has_value()) {
    rb_hash_aset(bucket,
                 rb_id2sym(rb_intern("history_retention_collection_default")),
                 entry.history_retention_collection_default.value() ? Qtrue : Qfalse);
  }
  if (const auto& val = entry.history_retention_bytes; val.has_value()) {
    rb_hash_aset(bucket, rb_id2sym(rb_intern("history_retention_bytes")), ULONG2NUM(val.value()));
  }
  if (const auto& val = entry.history_retention_duration; val.has_value()) {
    rb_hash_aset(
      bucket, rb_id2sym(rb_intern("history_retention_duration")), ULONG2NUM(val.value()));
  }
  if (const auto& val = entry.num_vbuckets; val.has_value()) {
    rb_hash_aset(bucket, rb_id2sym(rb_intern("num_vbuckets")), USHORT2NUM(val.value()));
  }

  VALUE capabilities = rb_ary_new_capa(static_cast<long>(entry.capabilities.size()));
  for (const auto& capa : entry.capabilities) {
    rb_ary_push(capabilities, cb_str_new(capa));
  }
  rb_hash_aset(bucket, rb_id2sym(rb_intern("capabilities")), capabilities);
  VALUE nodes = rb_ary_new_capa(static_cast<long>(entry.nodes.size()));
  for (const auto& n : entry.nodes) {
    VALUE node = rb_hash_new();
    rb_hash_aset(node, rb_id2sym(rb_intern("status")), cb_str_new(n.status));
    rb_hash_aset(node, rb_id2sym(rb_intern("hostname")), cb_str_new(n.hostname));
    rb_hash_aset(node, rb_id2sym(rb_intern("version")), cb_str_new(n.version));
    rb_ary_push(nodes, node);
  }
  rb_hash_aset(bucket, rb_id2sym(rb_intern("nodes")), nodes);
}

VALUE
cb_Backend_bucket_get_all(VALUE self, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::bucket_get_all_request req{};
    cb_extract_timeout(req, options);
    std::promise<core::operations::management::bucket_get_all_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, "unable to get list of the buckets of the cluster");
    }

    VALUE res = rb_ary_new_capa(static_cast<long>(resp.buckets.size()));
    for (const auto& entry : resp.buckets) {
      VALUE bucket = rb_hash_new();
      cb_extract_bucket_settings(entry, bucket);
      rb_ary_push(res, bucket);
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
cb_Backend_bucket_get(VALUE self, VALUE bucket_name, VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket_name, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::bucket_get_request req{ cb_string_new(bucket_name) };
    cb_extract_timeout(req, options);
    std::promise<core::operations::management::bucket_get_response> promise;
    auto f = promise.get_future();
    cluster.execute(req, [promise = std::move(promise)](auto&& resp) mutable {
      promise.set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format("unable to locate bucket \"{}\" on the cluster", req.name));
    }

    VALUE res = rb_hash_new();
    cb_extract_bucket_settings(resp.bucket, res);
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
init_buckets(VALUE cBackend)
{
  rb_define_method(cBackend, "bucket_create", cb_Backend_bucket_create, 2);
  rb_define_method(cBackend, "bucket_update", cb_Backend_bucket_update, 2);
  rb_define_method(cBackend, "bucket_drop", cb_Backend_bucket_drop, 2);
  rb_define_method(cBackend, "bucket_flush", cb_Backend_bucket_flush, 2);
  rb_define_method(cBackend, "bucket_get_all", cb_Backend_bucket_get_all, 1);
  rb_define_method(cBackend, "bucket_get", cb_Backend_bucket_get, 2);
}
} // namespace couchbase::ruby
