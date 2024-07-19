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
#include <core/management/analytics_link_azure_blob_external.hxx>
#include <core/management/analytics_link_couchbase_remote.hxx>
#include <core/management/analytics_link_s3_external.hxx>
#include <core/operations/document_analytics.hxx>
#include <core/operations/management/analytics_dataset_create.hxx>
#include <core/operations/management/analytics_dataset_drop.hxx>
#include <core/operations/management/analytics_dataset_get_all.hxx>
#include <core/operations/management/analytics_dataverse_create.hxx>
#include <core/operations/management/analytics_dataverse_drop.hxx>
#include <core/operations/management/analytics_get_pending_mutations.hxx>
#include <core/operations/management/analytics_index_create.hxx>
#include <core/operations/management/analytics_index_drop.hxx>
#include <core/operations/management/analytics_index_get_all.hxx>
#include <core/operations/management/analytics_link_connect.hxx>
#include <core/operations/management/analytics_link_create.hxx>
#include <core/operations/management/analytics_link_disconnect.hxx>
#include <core/operations/management/analytics_link_drop.hxx>
#include <core/operations/management/analytics_link_get_all.hxx>
#include <core/operations/management/analytics_link_replace.hxx>

#include <fmt/core.h>

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
cb_Backend_analytics_get_pending_mutations(VALUE self, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  try {
    core::operations::management::analytics_get_pending_mutations_request req{};
    cb_extract_timeout(req, options);
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_get_pending_mutations_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(resp.ctx, "unable to get pending mutations for the analytics service");
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to get pending mutations for the analytics service ({}: {})",
                      first_error.code,
                      first_error.message));
      }
    }
    VALUE res = rb_hash_new();
    for (const auto& [name, counter] : resp.stats) {
      rb_hash_aset(res, cb_str_new(name), ULL2NUM(counter));
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
cb_Backend_analytics_dataset_get_all(VALUE self, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  try {
    core::operations::management::analytics_dataset_get_all_request req{};
    cb_extract_timeout(req, options);
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_dataset_get_all_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(resp.ctx, "unable to fetch all datasets");
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to fetch all datasets ({}: {})",
                                   first_error.code,
                                   first_error.message));
      }
    }
    VALUE res = rb_ary_new_capa(static_cast<long>(resp.datasets.size()));
    for (const auto& ds : resp.datasets) {
      VALUE dataset = rb_hash_new();
      rb_hash_aset(dataset, rb_id2sym(rb_intern("name")), cb_str_new(ds.name));
      rb_hash_aset(dataset, rb_id2sym(rb_intern("dataverse_name")), cb_str_new(ds.dataverse_name));
      rb_hash_aset(dataset, rb_id2sym(rb_intern("link_name")), cb_str_new(ds.link_name));
      rb_hash_aset(dataset, rb_id2sym(rb_intern("bucket_name")), cb_str_new(ds.bucket_name));
      rb_ary_push(res, dataset);
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
cb_Backend_analytics_dataset_drop(VALUE self, VALUE dataset_name, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(dataset_name, T_STRING);

  try {
    core::operations::management::analytics_dataset_drop_request req{};
    cb_extract_timeout(req, options);
    req.dataset_name = cb_string_new(dataset_name);
    VALUE dataverse_name = Qnil;
    cb_extract_option_string(dataverse_name, options, "dataverse_name");
    if (!NIL_P(dataverse_name)) {
      req.dataverse_name = cb_string_new(dataverse_name);
    }
    cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_dataset_drop_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });

    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to drop dataset `{}`.`{}`", req.dataverse_name, req.dataset_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to drop dataset `{}`.`{}` ({}: {})",
                                   req.dataverse_name,
                                   req.dataset_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_Backend_analytics_dataset_create(VALUE self,
                                    VALUE dataset_name,
                                    VALUE bucket_name,
                                    VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(dataset_name, T_STRING);
  Check_Type(bucket_name, T_STRING);

  try {
    core::operations::management::analytics_dataset_create_request req{};
    cb_extract_timeout(req, options);
    req.dataset_name = cb_string_new(dataset_name);
    req.bucket_name = cb_string_new(bucket_name);
    cb_extract_option_string(req.condition, options, "condition");
    cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
    cb_extract_option_bool(req.ignore_if_exists, options, "ignore_if_exists");
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_dataset_create_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });

    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to create dataset `{}`.`{}`", req.dataverse_name, req.dataset_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to create dataset `{}`.`{}` ({}: {})",
                                   req.dataverse_name,
                                   req.dataset_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_Backend_analytics_dataverse_drop(VALUE self, VALUE dataverse_name, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(dataverse_name, T_STRING);

  try {
    core::operations::management::analytics_dataverse_drop_request req{};
    cb_extract_timeout(req, options);
    req.dataverse_name = cb_string_new(dataverse_name);
    cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_dataverse_drop_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(resp.ctx, fmt::format("unable to drop dataverse `{}`", req.dataverse_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to drop dataverse `{}` ({}: {})",
                                   req.dataverse_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_Backend_analytics_dataverse_create(VALUE self, VALUE dataverse_name, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(dataverse_name, T_STRING);
  if (!NIL_P(dataverse_name)) {
    Check_Type(dataverse_name, T_STRING);
  }

  try {
    core::operations::management::analytics_dataverse_create_request req{};
    cb_extract_timeout(req, options);
    req.dataverse_name = cb_string_new(dataverse_name);
    cb_extract_option_bool(req.ignore_if_exists, options, "ignore_if_exists");
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_dataverse_create_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to create dataverse `{}`", req.dataverse_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to create dataverse `{}` ({}: {})",
                                   req.dataverse_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_Backend_analytics_index_get_all(VALUE self, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  try {
    core::operations::management::analytics_index_get_all_request req{};
    cb_extract_timeout(req, options);
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_index_get_all_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(resp.ctx, "unable to fetch all indexes");
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to fetch all indexes ({}: {})",
                                   first_error.code,
                                   first_error.message));
      }
    }
    VALUE res = rb_ary_new_capa(static_cast<long>(resp.indexes.size()));
    for (const auto& idx : resp.indexes) {
      VALUE index = rb_hash_new();
      rb_hash_aset(index, rb_id2sym(rb_intern("name")), cb_str_new(idx.name));
      rb_hash_aset(index, rb_id2sym(rb_intern("dataset_name")), cb_str_new(idx.dataset_name));
      rb_hash_aset(index, rb_id2sym(rb_intern("dataverse_name")), cb_str_new(idx.dataverse_name));
      rb_hash_aset(index, rb_id2sym(rb_intern("is_primary")), idx.is_primary ? Qtrue : Qfalse);
      rb_ary_push(res, index);
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
cb_Backend_analytics_index_create(VALUE self,
                                  VALUE index_name,
                                  VALUE dataset_name,
                                  VALUE fields,
                                  VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(index_name, T_STRING);
  Check_Type(dataset_name, T_STRING);
  Check_Type(fields, T_ARRAY);

  try {
    core::operations::management::analytics_index_create_request req{};
    cb_extract_timeout(req, options);
    req.index_name = cb_string_new(index_name);
    req.dataset_name = cb_string_new(dataset_name);
    auto fields_num = static_cast<std::size_t>(RARRAY_LEN(fields));
    for (std::size_t i = 0; i < fields_num; ++i) {
      VALUE entry = rb_ary_entry(fields, static_cast<long>(i));
      Check_Type(entry, T_ARRAY);
      if (RARRAY_LEN(entry) == 2) {
        VALUE field = rb_ary_entry(entry, 0);
        VALUE type = rb_ary_entry(entry, 1);
        req.fields.try_emplace(cb_string_new(field), cb_string_new(type));
      }
    }

    cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
    cb_extract_option_bool(req.ignore_if_exists, options, "ignore_if_exists");
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_index_create_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to create index `{}` on `{}`.`{}`",
                                   req.index_name,
                                   req.dataverse_name,
                                   req.dataset_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to create index `{}` on `{}`.`{}` ({}: {})",
                                   req.index_name,
                                   req.dataverse_name,
                                   req.dataset_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_Backend_analytics_index_drop(VALUE self, VALUE index_name, VALUE dataset_name, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(index_name, T_STRING);
  Check_Type(dataset_name, T_STRING);

  try {
    core::operations::management::analytics_index_drop_request req{};
    cb_extract_timeout(req, options);
    req.index_name = cb_string_new(index_name);
    req.dataset_name = cb_string_new(dataset_name);
    cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
    cb_extract_option_bool(req.ignore_if_does_not_exist, options, "ignore_if_does_not_exist");
    auto promise =
      std::make_shared<std::promise<core::operations::management::analytics_index_drop_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(resp.ctx,
                       fmt::format("unable to drop index `{}`.`{}`.`{}`",
                                   req.dataverse_name,
                                   req.dataset_name,
                                   req.index_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to drop index `{}`.`{}`.`{}` ({}: {})",
                                   req.dataverse_name,
                                   req.dataset_name,
                                   req.index_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_Backend_analytics_link_connect(VALUE self, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  try {
    core::operations::management::analytics_link_connect_request req{};
    cb_extract_timeout(req, options);
    cb_extract_option_string(req.link_name, options, "link_name");
    cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
    cb_extract_option_bool(req.force, options, "force");
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_link_connect_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });

    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to connect link `{}` on `{}`", req.link_name, req.dataverse_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to connect link `{}` on `{}` ({}: {})",
                                   req.link_name,
                                   req.dataverse_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_Backend_analytics_link_disconnect(VALUE self, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  try {
    core::operations::management::analytics_link_disconnect_request req{};
    cb_extract_timeout(req, options);
    cb_extract_option_string(req.link_name, options, "link_name");
    cb_extract_option_string(req.dataverse_name, options, "dataverse_name");
    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_link_disconnect_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to disconnect link `{}` on `{}`", req.link_name, req.dataverse_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to disconnect link `{}` on `{}` ({}: {})",
                                   req.link_name,
                                   req.dataverse_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_fill_link(core::management::analytics::couchbase_remote_link& dst, VALUE src)
{
  cb_extract_option_string(dst.link_name, src, "link_name");
  cb_extract_option_string(dst.dataverse, src, "dataverse");
  cb_extract_option_string(dst.hostname, src, "hostname");
  cb_extract_option_string(dst.username, src, "username");
  cb_extract_option_string(dst.password, src, "password");
  VALUE encryption_level = Qnil;
  cb_extract_option_symbol(encryption_level, src, "encryption_level");
  if (NIL_P(encryption_level)) {
    encryption_level = rb_id2sym(rb_intern("none"));
  }
  if (ID level = rb_sym2id(encryption_level); level == rb_intern("none")) {
    dst.encryption.level = core::management::analytics::couchbase_link_encryption_level::none;
  } else if (level == rb_intern("half")) {
    dst.encryption.level = core::management::analytics::couchbase_link_encryption_level::half;
  } else if (level == rb_intern("full")) {
    dst.encryption.level = core::management::analytics::couchbase_link_encryption_level::full;
  }
  cb_extract_option_string(dst.encryption.certificate, src, "certificate");
  cb_extract_option_string(dst.encryption.client_certificate, src, "client_certificate");
  cb_extract_option_string(dst.encryption.client_key, src, "client_key");
}

void
cb_fill_link(core::management::analytics::azure_blob_external_link& dst, VALUE src)
{
  cb_extract_option_string(dst.link_name, src, "link_name");
  cb_extract_option_string(dst.dataverse, src, "dataverse");
  cb_extract_option_string(dst.connection_string, src, "connection_string");
  cb_extract_option_string(dst.account_name, src, "account_name");
  cb_extract_option_string(dst.account_key, src, "account_key");
  cb_extract_option_string(dst.shared_access_signature, src, "shared_access_signature");
  cb_extract_option_string(dst.blob_endpoint, src, "blob_endpoint");
  cb_extract_option_string(dst.endpoint_suffix, src, "endpoint_suffix");
}

void
cb_fill_link(core::management::analytics::s3_external_link& dst, VALUE src)
{
  cb_extract_option_string(dst.link_name, src, "link_name");
  cb_extract_option_string(dst.dataverse, src, "dataverse");
  cb_extract_option_string(dst.access_key_id, src, "access_key_id");
  cb_extract_option_string(dst.secret_access_key, src, "secret_access_key");
  cb_extract_option_string(dst.session_token, src, "session_token");
  cb_extract_option_string(dst.region, src, "region");
  cb_extract_option_string(dst.service_endpoint, src, "service_endpoint");
}

VALUE
cb_Backend_analytics_link_create(VALUE self, VALUE link, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    VALUE link_type = Qnil;
    cb_extract_option_symbol(link_type, link, "type");
    if (ID type = rb_sym2id(link_type); type == rb_intern("couchbase")) {
      core::operations::management::analytics_link_create_request<
        core::management::analytics::couchbase_remote_link>
        req{};
      cb_extract_timeout(req, options);
      cb_fill_link(req.link, link);

      auto promise = std::make_shared<
        std::promise<core::operations::management::analytics_link_create_response>>();
      auto f = promise->get_future();
      cluster->execute(req, [promise](auto&& resp) {
        promise->set_value(std::forward<decltype(resp)>(resp));
      });

      if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
        if (resp.errors.empty()) {
          cb_throw_error(resp.ctx,
                         fmt::format("unable to create couchbase_remote link `{}` on `{}`",
                                     req.link.link_name,
                                     req.link.dataverse));
        } else {
          const auto& first_error = resp.errors.front();
          cb_throw_error(resp.ctx,
                         fmt::format("unable to create couchbase_remote link `{}` on `{}` ({}: {})",
                                     req.link.link_name,
                                     req.link.dataverse,
                                     first_error.code,
                                     first_error.message));
        }
      }

    } else if (type == rb_intern("azureblob")) {
      core::operations::management::analytics_link_create_request<
        core::management::analytics::azure_blob_external_link>
        req{};
      cb_extract_timeout(req, options);
      cb_fill_link(req.link, link);

      auto promise = std::make_shared<
        std::promise<core::operations::management::analytics_link_create_response>>();
      auto f = promise->get_future();
      cluster->execute(req, [promise](auto&& resp) {
        promise->set_value(std::forward<decltype(resp)>(resp));
      });

      if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
        if (resp.errors.empty()) {
          cb_throw_error(resp.ctx,
                         fmt::format("unable to create azure_blob_external link `{}` on `{}`",
                                     req.link.link_name,
                                     req.link.dataverse));
        } else {
          const auto& first_error = resp.errors.front();
          cb_throw_error(
            resp.ctx,
            fmt::format("unable to create azure_blob_external link `{}` on `{}` ({}: {})",
                        req.link.link_name,
                        req.link.dataverse,
                        first_error.code,
                        first_error.message));
        }
      }

    } else if (type == rb_intern("s3")) {
      core::operations::management::analytics_link_create_request<
        core::management::analytics::s3_external_link>
        req{};
      cb_extract_timeout(req, options);
      cb_fill_link(req.link, link);

      auto promise = std::make_shared<
        std::promise<core::operations::management::analytics_link_create_response>>();
      auto f = promise->get_future();
      cluster->execute(req, [promise](auto&& resp) {
        promise->set_value(std::forward<decltype(resp)>(resp));
      });

      if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
        if (resp.errors.empty()) {
          cb_throw_error(resp.ctx,
                         fmt::format("unable to create s3_external link `{}` on `{}`",
                                     req.link.link_name,
                                     req.link.dataverse));
        } else {
          const auto& first_error = resp.errors.front();
          cb_throw_error(resp.ctx,
                         fmt::format("unable to create s3_external link `{}` on `{}` ({}: {})",
                                     req.link.link_name,
                                     req.link.dataverse,
                                     first_error.code,
                                     first_error.message));
        }
      }
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
cb_Backend_analytics_link_replace(VALUE self, VALUE link, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    VALUE link_type = Qnil;
    cb_extract_option_symbol(link_type, link, "type");

    if (ID type = rb_sym2id(link_type); type == rb_intern("couchbase")) {
      core::operations::management::analytics_link_replace_request<
        core::management::analytics::couchbase_remote_link>
        req{};
      cb_extract_timeout(req, options);
      cb_fill_link(req.link, link);

      auto promise = std::make_shared<
        std::promise<core::operations::management::analytics_link_replace_response>>();
      auto f = promise->get_future();
      cluster->execute(req, [promise](auto&& resp) {
        promise->set_value(std::forward<decltype(resp)>(resp));
      });

      if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
        if (resp.errors.empty()) {
          cb_throw_error(resp.ctx,
                         fmt::format("unable to replace couchbase_remote link `{}` on `{}`",
                                     req.link.link_name,
                                     req.link.dataverse));
        } else {
          const auto& first_error = resp.errors.front();
          cb_throw_error(
            resp.ctx,
            fmt::format("unable to replace couchbase_remote link `{}` on `{}` ({}: {})",
                        req.link.link_name,
                        req.link.dataverse,
                        first_error.code,
                        first_error.message));
        }
      }

    } else if (type == rb_intern("azureblob")) {
      core::operations::management::analytics_link_replace_request<
        core::management::analytics::azure_blob_external_link>
        req{};
      cb_extract_timeout(req, options);
      cb_fill_link(req.link, link);

      auto promise = std::make_shared<
        std::promise<core::operations::management::analytics_link_replace_response>>();
      auto f = promise->get_future();
      cluster->execute(req, [promise](auto&& resp) {
        promise->set_value(std::forward<decltype(resp)>(resp));
      });

      if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
        if (resp.errors.empty()) {
          cb_throw_error(resp.ctx,
                         fmt::format("unable to replace azure_blob_external link `{}` on `{}`",
                                     req.link.link_name,
                                     req.link.dataverse));
        } else {
          const auto& first_error = resp.errors.front();
          cb_throw_error(
            resp.ctx,
            fmt::format("unable to replace azure_blob_external link `{}` on `{}` ({}: {})",
                        req.link.link_name,
                        req.link.dataverse,
                        first_error.code,
                        first_error.message));
        }
      }

    } else if (type == rb_intern("s3")) {
      core::operations::management::analytics_link_replace_request<
        core::management::analytics::s3_external_link>
        req{};
      cb_extract_timeout(req, options);
      cb_fill_link(req.link, link);

      auto promise = std::make_shared<
        std::promise<core::operations::management::analytics_link_replace_response>>();
      auto f = promise->get_future();
      cluster->execute(req, [promise](auto&& resp) {
        promise->set_value(std::forward<decltype(resp)>(resp));
      });

      if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
        if (resp.errors.empty()) {
          cb_throw_error(resp.ctx,
                         fmt::format("unable to replace s3_external link `{}` on `{}`",
                                     req.link.link_name,
                                     req.link.dataverse));
        } else {
          const auto& first_error = resp.errors.front();
          cb_throw_error(resp.ctx,
                         fmt::format("unable to replace s3_external link `{}` on `{}` ({}: {})",
                                     req.link.link_name,
                                     req.link.dataverse,
                                     first_error.code,
                                     first_error.message));
        }
      }
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
cb_Backend_analytics_link_drop(VALUE self, VALUE link, VALUE dataverse, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(link, T_STRING);
  Check_Type(dataverse, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::analytics_link_drop_request req{};
    cb_extract_timeout(req, options);

    req.link_name = cb_string_new(link);
    req.dataverse_name = cb_string_new(dataverse);

    auto promise =
      std::make_shared<std::promise<core::operations::management::analytics_link_drop_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });

    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format("unable to drop link `{}` on `{}`", req.link_name, req.dataverse_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to drop link `{}` on `{}` ({}: {})",
                                   req.link_name,
                                   req.dataverse_name,
                                   first_error.code,
                                   first_error.message));
      }
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
cb_Backend_analytics_link_get_all(VALUE self, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::management::analytics_link_get_all_request req{};
    cb_extract_timeout(req, options);

    cb_extract_option_string(req.link_type, options, "link_type");
    cb_extract_option_string(req.link_name, options, "link_name");
    cb_extract_option_string(req.dataverse_name, options, "dataverse");

    auto promise = std::make_shared<
      std::promise<core::operations::management::analytics_link_get_all_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);

    if (resp.ctx.ec) {
      if (resp.errors.empty()) {
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to retrieve links type={}, dataverse="{}",  name="{}")",
                      req.link_type,
                      req.link_name,
                      req.dataverse_name));
      } else {
        const auto& first_error = resp.errors.front();
        cb_throw_error(
          resp.ctx,
          fmt::format(R"(unable to retrieve links type={}, dataverse="{}",  name="{}" ({}: {}))",
                      req.link_type,
                      req.link_name,
                      req.dataverse_name,
                      first_error.code,
                      first_error.message));
      }
    }

    VALUE res = rb_ary_new_capa(
      static_cast<long>(resp.couchbase.size() + resp.s3.size() + resp.azure_blob.size()));
    for (const auto& link : resp.couchbase) {
      VALUE row = rb_hash_new();
      rb_hash_aset(row, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("couchbase")));
      rb_hash_aset(row, rb_id2sym(rb_intern("dataverse")), cb_str_new(link.dataverse));
      rb_hash_aset(row, rb_id2sym(rb_intern("link_name")), cb_str_new(link.link_name));
      rb_hash_aset(row, rb_id2sym(rb_intern("hostname")), cb_str_new(link.hostname));
      switch (link.encryption.level) {
        case core::management::analytics::couchbase_link_encryption_level::none:
          rb_hash_aset(row, rb_id2sym(rb_intern("encryption_level")), rb_id2sym(rb_intern("none")));
          break;
        case core::management::analytics::couchbase_link_encryption_level::half:
          rb_hash_aset(row, rb_id2sym(rb_intern("encryption_level")), rb_id2sym(rb_intern("half")));
          break;
        case core::management::analytics::couchbase_link_encryption_level::full:
          rb_hash_aset(row, rb_id2sym(rb_intern("encryption_level")), rb_id2sym(rb_intern("full")));
          break;
      }
      rb_hash_aset(row, rb_id2sym(rb_intern("username")), cb_str_new(link.username));
      rb_hash_aset(
        row, rb_id2sym(rb_intern("certificate")), cb_str_new(link.encryption.certificate));
      rb_hash_aset(row,
                   rb_id2sym(rb_intern("client_certificate")),
                   cb_str_new(link.encryption.client_certificate));
      rb_ary_push(res, row);
    }
    for (const auto& link : resp.s3) {
      VALUE row = rb_hash_new();
      rb_hash_aset(row, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("s3")));
      rb_hash_aset(row, rb_id2sym(rb_intern("dataverse")), cb_str_new(link.dataverse));
      rb_hash_aset(row, rb_id2sym(rb_intern("link_name")), cb_str_new(link.link_name));
      rb_hash_aset(row, rb_id2sym(rb_intern("access_key_id")), cb_str_new(link.access_key_id));
      rb_hash_aset(row, rb_id2sym(rb_intern("region")), cb_str_new(link.region));
      rb_hash_aset(
        row, rb_id2sym(rb_intern("service_endpoint")), cb_str_new(link.service_endpoint));
      rb_ary_push(res, row);
    }
    for (const auto& link : resp.azure_blob) {
      VALUE row = rb_hash_new();
      rb_hash_aset(row, rb_id2sym(rb_intern("type")), rb_id2sym(rb_intern("azureblob")));
      rb_hash_aset(row, rb_id2sym(rb_intern("dataverse")), cb_str_new(link.dataverse));
      rb_hash_aset(row, rb_id2sym(rb_intern("link_name")), cb_str_new(link.link_name));
      rb_hash_aset(row, rb_id2sym(rb_intern("account_name")), cb_str_new(link.account_name));
      rb_hash_aset(row, rb_id2sym(rb_intern("blob_endpoint")), cb_str_new(link.blob_endpoint));
      rb_hash_aset(row, rb_id2sym(rb_intern("endpoint_suffix")), cb_str_new(link.endpoint_suffix));
      rb_ary_push(res, row);
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

const char*
cb_analytics_status_str(core::operations::analytics_response::analytics_status status)
{
  switch (status) {
    case core::operations::analytics_response::running:
      return "running";

    case core::operations::analytics_response::success:
      return "success";
    case core::operations::analytics_response::errors:
      return "errors";
    case core::operations::analytics_response::completed:
      return "completed";
    case core::operations::analytics_response::stopped:
      return "stopped";
    case core::operations::analytics_response::timedout:
      return "timedout";
    case core::operations::analytics_response::closed:
      return "closed";
    case core::operations::analytics_response::fatal:
      return "fatal";
    case core::operations::analytics_response::aborted:
      return "aborted";
    case core::operations::analytics_response::unknown:
      return "unknown";
    default:
      break;
  }
  return "unknown";
}

int
cb_for_each_named_param_analytics(VALUE key, VALUE value, VALUE arg)
{
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  auto* preq = reinterpret_cast<core::operations::analytics_request*>(arg);
  cb_check_type(key, T_STRING);
  cb_check_type(value, T_STRING);
  preq->named_parameters[cb_string_new(key)] = cb_string_new(value);
  return ST_CONTINUE;
}

VALUE
cb_Backend_document_analytics(VALUE self, VALUE statement, VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(statement, T_STRING);
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::analytics_request req;
    req.statement = cb_string_new(statement);
    if (VALUE client_context_id = rb_hash_aref(options, rb_id2sym(rb_intern("client_context_id")));
        !NIL_P(client_context_id)) {
      cb_check_type(client_context_id, T_STRING);
      req.client_context_id = cb_string_new(client_context_id);
    }
    cb_extract_timeout(req, options);
    cb_extract_option_bool(req.readonly, options, "readonly");
    cb_extract_option_bool(req.priority, options, "priority");
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
      rb_hash_foreach(
        named_params, cb_for_each_named_param_analytics, reinterpret_cast<VALUE>(&req));
    }
    if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency")));
        !NIL_P(scan_consistency)) {
      cb_check_type(scan_consistency, T_SYMBOL);
      if (ID type = rb_sym2id(scan_consistency); type == rb_intern("not_bounded")) {
        req.scan_consistency = core::analytics_scan_consistency::not_bounded;
      } else if (type == rb_intern("request_plus")) {
        req.scan_consistency = core::analytics_scan_consistency::request_plus;
      }
    }

    if (VALUE scope_qualifier = rb_hash_aref(options, rb_id2sym(rb_intern("scope_qualifier")));
        !NIL_P(scope_qualifier) && TYPE(scope_qualifier) == T_STRING) {
      req.scope_qualifier.emplace(cb_string_new(scope_qualifier));
    } else {
      VALUE scope_name = rb_hash_aref(options, rb_id2sym(rb_intern("scope_name")));
      if (!NIL_P(scope_name) && TYPE(scope_name) == T_STRING) {
        req.scope_name.emplace(cb_string_new(scope_name));
        VALUE bucket_name = rb_hash_aref(options, rb_id2sym(rb_intern("bucket_name")));
        if (NIL_P(bucket_name)) {
          throw ruby_exception(
            exc_invalid_argument(),
            fmt::format("bucket must be specified for analytics query in scope \"{}\"",
                        req.scope_name.value()));
        }
        req.bucket_name.emplace(cb_string_new(bucket_name));
      }
    }

    if (VALUE raw_params = rb_hash_aref(options, rb_id2sym(rb_intern("raw_parameters")));
        !NIL_P(raw_params)) {
      cb_check_type(raw_params, T_HASH);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      rb_hash_foreach(raw_params, cb_for_each_named_param_analytics, reinterpret_cast<VALUE>(&req));
    }

    auto promise = std::make_shared<std::promise<core::operations::analytics_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      if (!resp.meta.errors.empty()) {
        const auto& first_error = resp.meta.errors.front();
        cb_throw_error(resp.ctx,
                       fmt::format("unable to execute analytics query ({}: {})",
                                   first_error.code,
                                   first_error.message));
      } else {
        cb_throw_error(resp.ctx, "unable to execute analytics query");
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
    rb_hash_aset(meta,
                 rb_id2sym(rb_intern("status")),
                 rb_id2sym(rb_intern(cb_analytics_status_str(resp.meta.status))));
    rb_hash_aset(meta, rb_id2sym(rb_intern("request_id")), cb_str_new(resp.meta.request_id));
    rb_hash_aset(
      meta, rb_id2sym(rb_intern("client_context_id")), cb_str_new(resp.meta.client_context_id));
    if (resp.meta.signature) {
      rb_hash_aset(
        meta, rb_id2sym(rb_intern("signature")), cb_str_new(resp.meta.signature.value()));
    }
    VALUE metrics = rb_hash_new();
    rb_hash_aset(meta, rb_id2sym(rb_intern("metrics")), metrics);
    rb_hash_aset(
      metrics, rb_id2sym(rb_intern("elapsed_time")), resp.meta.metrics.elapsed_time.count());
    rb_hash_aset(
      metrics, rb_id2sym(rb_intern("execution_time")), resp.meta.metrics.execution_time.count());
    rb_hash_aset(
      metrics, rb_id2sym(rb_intern("result_count")), ULL2NUM(resp.meta.metrics.result_count));
    rb_hash_aset(
      metrics, rb_id2sym(rb_intern("result_size")), ULL2NUM(resp.meta.metrics.result_size));
    rb_hash_aset(
      metrics, rb_id2sym(rb_intern("error_count")), ULL2NUM(resp.meta.metrics.error_count));
    rb_hash_aset(metrics,
                 rb_id2sym(rb_intern("processed_objects")),
                 ULL2NUM(resp.meta.metrics.processed_objects));
    rb_hash_aset(
      metrics, rb_id2sym(rb_intern("warning_count")), ULL2NUM(resp.meta.metrics.warning_count));

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
init_analytics(VALUE cBackend)
{
  rb_define_method(cBackend, "document_analytics", cb_Backend_document_analytics, 2);

  // Management APIs
  rb_define_method(
    cBackend, "analytics_get_pending_mutations", cb_Backend_analytics_get_pending_mutations, 1);
  rb_define_method(cBackend, "analytics_dataverse_drop", cb_Backend_analytics_dataverse_drop, 2);
  rb_define_method(
    cBackend, "analytics_dataverse_create", cb_Backend_analytics_dataverse_create, 2);
  rb_define_method(cBackend, "analytics_dataset_create", cb_Backend_analytics_dataset_create, 3);
  rb_define_method(cBackend, "analytics_dataset_drop", cb_Backend_analytics_dataset_drop, 2);
  rb_define_method(cBackend, "analytics_dataset_get_all", cb_Backend_analytics_dataset_get_all, 1);
  rb_define_method(cBackend, "analytics_index_get_all", cb_Backend_analytics_index_get_all, 1);
  rb_define_method(cBackend, "analytics_index_create", cb_Backend_analytics_index_create, 4);
  rb_define_method(cBackend, "analytics_index_drop", cb_Backend_analytics_index_drop, 3);
  rb_define_method(cBackend, "analytics_link_connect", cb_Backend_analytics_link_connect, 1);
  rb_define_method(cBackend, "analytics_link_disconnect", cb_Backend_analytics_link_disconnect, 1);
  rb_define_method(cBackend, "analytics_link_create", cb_Backend_analytics_link_create, 2);
  rb_define_method(cBackend, "analytics_link_replace", cb_Backend_analytics_link_replace, 2);
  rb_define_method(cBackend, "analytics_link_drop", cb_Backend_analytics_link_drop, 3);
  rb_define_method(cBackend, "analytics_link_get_all", cb_Backend_analytics_link_get_all, 1);
}
} // namespace couchbase::ruby
