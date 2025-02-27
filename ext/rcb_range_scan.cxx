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

#include <core/agent_group.hxx>
#include <core/agent_group_config.hxx>
#include <core/cluster.hxx>
#include <core/range_scan_options.hxx>
#include <core/range_scan_orchestrator.hxx>
#include <core/range_scan_orchestrator_options.hxx>
#include <core/scan_result.hxx>
#include <couchbase/error_codes.hxx>

#include <spdlog/fmt/bundled/core.h>

#include <future>

#include <gsl/util>
#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_range_scan.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{
struct cb_core_scan_result_data {
  std::unique_ptr<couchbase::core::scan_result> scan_result;
};

void
cb_CoreScanResult_mark(void* ptr)
{
  /* No embedded Ruby objects */
}

void
cb_CoreScanResult_free(void* ptr)
{
  auto* data = static_cast<cb_core_scan_result_data*>(ptr);
  if (data->scan_result != nullptr && !data->scan_result->is_cancelled()) {
    data->scan_result->cancel();
  }
  data->scan_result.reset();
  ruby_xfree(data);
}

const rb_data_type_t cb_core_scan_result_type {
    .wrap_struct_name = "Couchbase/Backend/CoreScanResult",
    .function = {
      .dmark = cb_CoreScanResult_mark,
      .dfree = cb_CoreScanResult_free,
    },
    .data = nullptr,
#ifdef RUBY_TYPED_FREE_IMMEDIATELY
    .flags = RUBY_TYPED_FREE_IMMEDIATELY,
#endif
};

VALUE
cb_CoreScanResult_allocate(VALUE klass)
{
  cb_core_scan_result_data* data = nullptr;
  VALUE obj =
    TypedData_Make_Struct(klass, cb_core_scan_result_data, &cb_core_scan_result_type, data);
  return obj;
}

VALUE
cb_CoreScanResult_is_cancelled(VALUE self)
{
  cb_core_scan_result_data* data = nullptr;
  TypedData_Get_Struct(self, cb_core_scan_result_data, &cb_core_scan_result_type, data);
  auto resp = data->scan_result->is_cancelled();
  if (resp) {
    return Qtrue;
  }
  return Qfalse;
}

VALUE
cb_CoreScanResult_cancel(VALUE self)
{
  cb_core_scan_result_data* data = nullptr;
  TypedData_Get_Struct(self, cb_core_scan_result_data, &cb_core_scan_result_type, data);
  data->scan_result->cancel();
  return Qnil;
}

VALUE
cb_CoreScanResult_next_item(VALUE self)
{
  try {
    cb_core_scan_result_data* data = nullptr;
    TypedData_Get_Struct(self, cb_core_scan_result_data, &cb_core_scan_result_type, data);
    std::promise<tl::expected<couchbase::core::range_scan_item, std::error_code>> promise;
    auto f = promise.get_future();
    data->scan_result->next([promise = std::move(promise)](couchbase::core::range_scan_item item,
                                                           std::error_code ec) mutable {
      if (ec) {
        return promise.set_value(tl::unexpected(ec));
      }
      return promise.set_value(item);
    });
    auto resp = cb_wait_for_future(f);
    if (!resp.has_value()) {
      // If the error code is range_scan_completed return nil without raising an exception (nil
      // signifies that there are no more items)
      if (resp.error() != couchbase::errc::key_value::range_scan_completed) {
        cb_throw_error_code(resp.error(), "unable to fetch next scan item");
      }
      // Release ownership of scan_result unique pointer
      return Qnil;
    }
    auto item = resp.value();
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("id")), cb_str_new(item.key));
    if (item.body.has_value()) {
      auto body = item.body.value();
      rb_hash_aset(res, rb_id2sym(rb_intern("id")), cb_str_new(item.key));
      rb_hash_aset(res, rb_id2sym(rb_intern("encoded")), cb_str_new(body.value));
      rb_hash_aset(res, rb_id2sym(rb_intern("cas")), cb_cas_to_num(body.cas));
      rb_hash_aset(res, rb_id2sym(rb_intern("flags")), UINT2NUM(body.flags));
      rb_hash_aset(res, rb_id2sym(rb_intern("expiry")), UINT2NUM(body.expiry));
      rb_hash_aset(res, rb_id2sym(rb_intern("id_only")), Qfalse);
    } else {
      rb_hash_aset(res, rb_id2sym(rb_intern("id_only")), Qtrue);
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

VALUE cCoreScanResult;

VALUE
cb_Backend_document_scan_create(VALUE self,
                                VALUE bucket,
                                VALUE scope,
                                VALUE collection,
                                VALUE scan_type,
                                VALUE options)
{
  auto cluster = cb_backend_to_core_api_cluster(self);

  Check_Type(bucket, T_STRING);
  Check_Type(scope, T_STRING);
  Check_Type(collection, T_STRING);
  Check_Type(scan_type, T_HASH);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    couchbase::core::range_scan_orchestrator_options orchestrator_options{};
    cb_extract_timeout(orchestrator_options, options);
    cb_extract_option_bool(orchestrator_options.ids_only, options, "ids_only");
    cb_extract_option_number(orchestrator_options.batch_item_limit, options, "batch_item_limit");
    cb_extract_option_number(orchestrator_options.batch_byte_limit, options, "batch_byte_limit");
    cb_extract_option_number(orchestrator_options.concurrency, options, "concurrency");

    // Extracting the mutation state
    if (VALUE mutation_state = rb_hash_aref(options, rb_id2sym(rb_intern("mutation_state")));
        !NIL_P(mutation_state)) {
      cb_check_type(mutation_state, T_ARRAY);
      auto state_size = static_cast<std::size_t>(RARRAY_LEN(mutation_state));

      if (state_size > 0) {
        auto core_mut_state = couchbase::core::mutation_state{};
        core_mut_state.tokens.reserve(state_size);
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
          core_mut_state.tokens.emplace_back(
            NUM2ULL(partition_uuid),
            NUM2ULL(sequence_number),
            gsl::narrow_cast<std::uint16_t>(NUM2UINT(partition_id)),
            cb_string_new(bucket_name));
        }

        orchestrator_options.consistent_with = core_mut_state;
      }
    }

    auto bucket_name = cb_string_new(bucket);
    auto scope_name = cb_string_new(scope);
    auto collection_name = cb_string_new(collection);

    // Getting the operation agent
    auto agent_group = couchbase::core::agent_group(
      cluster.io_context(), couchbase::core::agent_group_config{ { cluster } });
    auto err = agent_group.open_bucket(bucket_name);
    if (err) {
      cb_throw_error_code(err, "unable to open bucket for range scan");
      return Qnil;
    }
    auto agent = agent_group.get_agent(bucket_name);
    if (!agent.has_value()) {
      rb_raise(exc_couchbase_error(),
               "Cannot perform scan operation. Unable to get operation agent");
      return Qnil;
    }

    // Getting the vbucket map
    std::promise<tl::expected<couchbase::core::topology::configuration, std::error_code>> promise;
    auto f = promise.get_future();
    cluster.with_bucket_configuration(
      bucket_name, [promise = std::move(promise)](std::error_code ec, const auto& config) mutable {
        if (ec) {
          return promise.set_value(tl::unexpected(ec));
        }
        promise.set_value(*config);
      });
    auto config = cb_wait_for_future(f);
    if (!config.has_value()) {
      rb_raise(exc_couchbase_error(),
               "Cannot perform scan operation. Unable to get bucket configuration");
      return Qnil;
    }
    if (!config->capabilities.supports_range_scan()) {
      rb_raise(exc_feature_not_available(), "Server does not support key-value scan operations");
      return Qnil;
    }
    auto vbucket_map = config->vbmap;
    if (!vbucket_map || vbucket_map->empty()) {
      rb_raise(exc_couchbase_error(), "Cannot perform scan operation. Unable to get vbucket map");
      return Qnil;
    }

    // Constructing the scan type
    std::variant<std::monostate,
                 couchbase::core::range_scan,
                 couchbase::core::prefix_scan,
                 couchbase::core::sampling_scan>
      core_scan_type{};
    ID scan_type_id = rb_sym2id(rb_hash_aref(scan_type, rb_id2sym(rb_intern("scan_type"))));
    if (scan_type_id == rb_intern("range")) {
      auto range_scan = couchbase::core::range_scan{};

      VALUE from_hash = rb_hash_aref(scan_type, rb_id2sym(rb_intern("from")));
      VALUE to_hash = rb_hash_aref(scan_type, rb_id2sym(rb_intern("to")));

      if (!NIL_P(from_hash)) {
        Check_Type(from_hash, T_HASH);
        range_scan.from = couchbase::core::scan_term{};
        cb_extract_option_string(range_scan.from->term, from_hash, "term");
        cb_extract_option_bool(range_scan.from->exclusive, from_hash, "exclusive");
      }
      if (!NIL_P(to_hash)) {
        Check_Type(to_hash, T_HASH);
        range_scan.to = couchbase::core::scan_term{};
        cb_extract_option_string(range_scan.to->term, to_hash, "term");
        cb_extract_option_bool(range_scan.to->exclusive, to_hash, "exclusive");
      }
      core_scan_type = range_scan;
    } else if (scan_type_id == rb_intern("prefix")) {
      auto prefix_scan = couchbase::core::prefix_scan{};
      cb_extract_option_string(prefix_scan.prefix, scan_type, "prefix");
      core_scan_type = prefix_scan;
    } else if (scan_type_id == rb_intern("sampling")) {
      auto sampling_scan = couchbase::core::sampling_scan{};
      cb_extract_option_number(sampling_scan.limit, scan_type, "limit");
      cb_extract_option_number(sampling_scan.seed, scan_type, "seed");
      core_scan_type = sampling_scan;
    } else {
      rb_raise(exc_invalid_argument(), "Invalid scan operation type");
    }

    auto orchestrator = couchbase::core::range_scan_orchestrator(cluster.io_context(),
                                                                 agent.value(),
                                                                 vbucket_map.value(),
                                                                 scope_name,
                                                                 collection_name,
                                                                 core_scan_type,
                                                                 orchestrator_options);

    // Start the scan
    auto resp = orchestrator.scan();
    if (!resp.has_value()) {
      cb_throw_error_code(resp.error(), "unable to start scan");
    }

    // Wrap core scan_result inside Ruby ScanResult
    // Creating a Ruby CoreScanResult object *after* checking that no error occurred during
    // orchestrator.scan()
    VALUE core_scan_result_obj = rb_class_new_instance(0, nullptr, cCoreScanResult);
    rb_ivar_set(core_scan_result_obj, rb_intern("@backend"), self);
    cb_core_scan_result_data* data = nullptr;
    TypedData_Get_Struct(
      core_scan_result_obj, cb_core_scan_result_data, &cb_core_scan_result_type, data);
    data->scan_result = std::make_unique<couchbase::core::scan_result>(resp.value());
    return core_scan_result_obj;

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
init_range_scan(VALUE mCouchbase, VALUE cBackend)
{
  rb_define_method(cBackend, "document_scan_create", cb_Backend_document_scan_create, 5);

  cCoreScanResult = rb_define_class_under(mCouchbase, "CoreScanResult", rb_cObject);
  rb_define_alloc_func(cCoreScanResult, cb_CoreScanResult_allocate);
  rb_define_method(cCoreScanResult, "next_item", cb_CoreScanResult_next_item, 0);
  rb_define_method(cCoreScanResult, "cancelled?", cb_CoreScanResult_is_cancelled, 0);
  rb_define_method(cCoreScanResult, "cancel", cb_CoreScanResult_cancel, 0);
}
} // namespace couchbase::ruby
