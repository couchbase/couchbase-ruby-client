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
#include <core/design_document_namespace_fmt.hxx>
#include <core/operations/document_view.hxx>
#include <core/operations/management/view_index_drop.hxx>
#include <core/operations/management/view_index_get.hxx>
#include <core/operations/management/view_index_get_all.hxx>
#include <core/operations/management/view_index_upsert.hxx>

#include <future>

#include <ruby.h>

#include "rcb_backend.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{
VALUE
cb_Backend_view_index_get_all(VALUE self, VALUE bucket_name, VALUE name_space, VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(name_space, T_SYMBOL);

  core::design_document_namespace ns{};
  if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
    ns = core::design_document_namespace::development;
  } else if (type == rb_intern("production")) {
    ns = core::design_document_namespace::production;
  } else {
    rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    return Qnil;
  }

  try {
    core::operations::management::view_index_get_all_request req{};
    req.bucket_name = cb_string_new(bucket_name);
    req.ns = ns;
    cb_extract_timeout(req, timeout);
    auto promise =
      std::make_shared<std::promise<core::operations::management::view_index_get_all_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx, "unable to get list of the design documents");
    }
    VALUE res = rb_ary_new_capa(static_cast<long>(resp.design_documents.size()));
    for (const auto& entry : resp.design_documents) {
      VALUE dd = rb_hash_new();
      rb_hash_aset(dd, rb_id2sym(rb_intern("name")), cb_str_new(entry.name));
      rb_hash_aset(dd, rb_id2sym(rb_intern("rev")), cb_str_new(entry.rev));
      switch (entry.ns) {
        case core::design_document_namespace::development:
          rb_hash_aset(dd, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("development")));
          break;
        case core::design_document_namespace::production:
          rb_hash_aset(dd, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("production")));
          break;
      }
      VALUE views = rb_hash_new();
      for (const auto& [name, view_entry] : entry.views) {
        VALUE view_name = cb_str_new(name);
        VALUE view = rb_hash_new();
        rb_hash_aset(view, rb_id2sym(rb_intern("name")), view_name);
        if (view_entry.map) {
          rb_hash_aset(view, rb_id2sym(rb_intern("map")), cb_str_new(view_entry.map.value()));
        }
        if (view_entry.reduce) {
          rb_hash_aset(view, rb_id2sym(rb_intern("reduce")), cb_str_new(view_entry.reduce.value()));
        }
        rb_hash_aset(views, view_name, view);
      }
      rb_hash_aset(dd, rb_id2sym(rb_intern("views")), views);
      rb_ary_push(res, dd);
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
cb_Backend_view_index_get(VALUE self,
                          VALUE bucket_name,
                          VALUE document_name,
                          VALUE name_space,
                          VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(document_name, T_STRING);
  Check_Type(name_space, T_SYMBOL);

  core::design_document_namespace ns{};
  if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
    ns = core::design_document_namespace::development;
  } else if (type == rb_intern("production")) {
    ns = core::design_document_namespace::production;
  } else {
    rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    return Qnil;
  }

  try {
    core::operations::management::view_index_get_request req{};
    req.bucket_name = cb_string_new(bucket_name);
    req.document_name = cb_string_new(document_name);
    req.ns = ns;
    cb_extract_timeout(req, timeout);
    auto promise =
      std::make_shared<std::promise<core::operations::management::view_index_get_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable to get design document "{}" ({}) on bucket "{}")",
                                 req.document_name,
                                 req.ns,
                                 req.bucket_name));
    }
    VALUE res = rb_hash_new();
    rb_hash_aset(res, rb_id2sym(rb_intern("name")), cb_str_new(resp.document.name));
    rb_hash_aset(res, rb_id2sym(rb_intern("rev")), cb_str_new(resp.document.rev));
    switch (resp.document.ns) {
      case core::design_document_namespace::development:
        rb_hash_aset(res, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("development")));
        break;
      case core::design_document_namespace::production:
        rb_hash_aset(res, rb_id2sym(rb_intern("namespace")), rb_id2sym(rb_intern("production")));
        break;
    }
    VALUE views = rb_hash_new();
    for (const auto& [name, view_entry] : resp.document.views) {
      VALUE view_name = cb_str_new(name);
      VALUE view = rb_hash_new();
      rb_hash_aset(view, rb_id2sym(rb_intern("name")), view_name);
      if (view_entry.map) {
        rb_hash_aset(view, rb_id2sym(rb_intern("map")), cb_str_new(view_entry.map.value()));
      }
      if (view_entry.reduce) {
        rb_hash_aset(view, rb_id2sym(rb_intern("reduce")), cb_str_new(view_entry.reduce.value()));
      }
      rb_hash_aset(views, view_name, view);
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("views")), views);
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
cb_Backend_view_index_drop(VALUE self,
                           VALUE bucket_name,
                           VALUE document_name,
                           VALUE name_space,
                           VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(document_name, T_STRING);
  Check_Type(name_space, T_SYMBOL);

  core::design_document_namespace ns{};
  if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
    ns = core::design_document_namespace::development;
  } else if (type == rb_intern("production")) {
    ns = core::design_document_namespace::production;
  } else {
    rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    return Qnil;
  }

  try {
    core::operations::management::view_index_drop_request req{};
    req.bucket_name = cb_string_new(bucket_name);
    req.document_name = cb_string_new(document_name);
    req.ns = ns;
    cb_extract_timeout(req, timeout);
    auto promise =
      std::make_shared<std::promise<core::operations::management::view_index_drop_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });

    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable to drop design document "{}" ({}) on bucket "{}")",
                                 req.document_name,
                                 req.ns,
                                 req.bucket_name));
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
cb_Backend_view_index_upsert(VALUE self,
                             VALUE bucket_name,
                             VALUE document,
                             VALUE name_space,
                             VALUE timeout)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(document, T_HASH);
  Check_Type(name_space, T_SYMBOL);

  core::design_document_namespace ns{};
  if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
    ns = core::design_document_namespace::development;
  } else if (type == rb_intern("production")) {
    ns = core::design_document_namespace::production;
  } else {
    rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    return Qnil;
  }

  try {
    core::operations::management::view_index_upsert_request req{};
    req.bucket_name = cb_string_new(bucket_name);
    req.document.ns = ns;
    if (VALUE document_name = rb_hash_aref(document, rb_id2sym(rb_intern("name")));
        !NIL_P(document_name)) {
      Check_Type(document_name, T_STRING);
      req.document.name = cb_string_new(document_name);
    }
    if (VALUE views = rb_hash_aref(document, rb_id2sym(rb_intern("views"))); !NIL_P(views)) {
      Check_Type(views, T_ARRAY);
      auto entries_num = static_cast<std::size_t>(RARRAY_LEN(views));
      for (std::size_t i = 0; i < entries_num; ++i) {
        VALUE entry = rb_ary_entry(views, static_cast<long>(i));
        Check_Type(entry, T_HASH);
        core::management::views::design_document::view view;
        VALUE name = rb_hash_aref(entry, rb_id2sym(rb_intern("name")));
        Check_Type(name, T_STRING);
        view.name = cb_string_new(name);
        if (VALUE map = rb_hash_aref(entry, rb_id2sym(rb_intern("map"))); !NIL_P(map)) {
          view.map.emplace(cb_string_new(map));
        }
        if (VALUE reduce = rb_hash_aref(entry, rb_id2sym(rb_intern("reduce"))); !NIL_P(reduce)) {
          view.reduce.emplace(cb_string_new(reduce));
        }
        req.document.views[view.name] = view;
      }
    }

    cb_extract_timeout(req, timeout);
    auto promise =
      std::make_shared<std::promise<core::operations::management::view_index_upsert_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });

    if (auto resp = cb_wait_for_future(f); resp.ctx.ec) {
      cb_throw_error(resp.ctx,
                     fmt::format(R"(unable to store design document "{}" ({}) on bucket "{}")",
                                 req.document.name,
                                 req.document.ns,
                                 req.bucket_name));
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
cb_Backend_document_view(VALUE self,
                         VALUE bucket_name,
                         VALUE design_document_name,
                         VALUE view_name,
                         VALUE name_space,
                         VALUE options)
{
  const auto& cluster = cb_backend_to_cluster(self);

  Check_Type(bucket_name, T_STRING);
  Check_Type(design_document_name, T_STRING);
  Check_Type(view_name, T_STRING);
  Check_Type(name_space, T_SYMBOL);

  core::design_document_namespace ns{};
  if (ID type = rb_sym2id(name_space); type == rb_intern("development")) {
    ns = core::design_document_namespace::development;
  } else if (type == rb_intern("production")) {
    ns = core::design_document_namespace::production;
  } else {
    rb_raise(rb_eArgError, "Unknown design document namespace: %+" PRIsVALUE, type);
    return Qnil;
  }
  if (!NIL_P(options)) {
    Check_Type(options, T_HASH);
  }

  try {
    core::operations::document_view_request req{};
    req.bucket_name = cb_string_new(bucket_name);
    req.document_name = cb_string_new(design_document_name);
    req.view_name = cb_string_new(view_name);
    req.ns = ns;
    cb_extract_timeout(req, options);
    if (!NIL_P(options)) {
      cb_extract_option_bool(req.debug, options, "debug");
      cb_extract_option_uint64(req.limit, options, "limit");
      cb_extract_option_uint64(req.skip, options, "skip");
      if (VALUE scan_consistency = rb_hash_aref(options, rb_id2sym(rb_intern("scan_consistency")));
          !NIL_P(scan_consistency)) {
        cb_check_type(scan_consistency, T_SYMBOL);
        if (ID consistency = rb_sym2id(scan_consistency);
            consistency == rb_intern("request_plus")) {
          req.consistency = core::view_scan_consistency::request_plus;
        } else if (consistency == rb_intern("update_after")) {
          req.consistency = core::view_scan_consistency::update_after;
        } else if (consistency == rb_intern("not_bounded")) {
          req.consistency = core::view_scan_consistency::not_bounded;
        }
      }
      if (VALUE key = rb_hash_aref(options, rb_id2sym(rb_intern("key"))); !NIL_P(key)) {
        cb_check_type(key, T_STRING);
        req.key.emplace(cb_string_new(key));
      }
      if (VALUE start_key = rb_hash_aref(options, rb_id2sym(rb_intern("start_key")));
          !NIL_P(start_key)) {
        cb_check_type(start_key, T_STRING);
        req.start_key.emplace(cb_string_new(start_key));
      }
      if (VALUE end_key = rb_hash_aref(options, rb_id2sym(rb_intern("end_key"))); !NIL_P(end_key)) {
        cb_check_type(end_key, T_STRING);
        req.end_key.emplace(cb_string_new(end_key));
      }
      if (VALUE start_key_doc_id = rb_hash_aref(options, rb_id2sym(rb_intern("start_key_doc_id")));
          !NIL_P(start_key_doc_id)) {
        cb_check_type(start_key_doc_id, T_STRING);
        req.start_key_doc_id.emplace(cb_string_new(start_key_doc_id));
      }
      if (VALUE end_key_doc_id = rb_hash_aref(options, rb_id2sym(rb_intern("end_key_doc_id")));
          !NIL_P(end_key_doc_id)) {
        cb_check_type(end_key_doc_id, T_STRING);
        req.end_key_doc_id.emplace(cb_string_new(end_key_doc_id));
      }
      if (VALUE inclusive_end = rb_hash_aref(options, rb_id2sym(rb_intern("inclusive_end")));
          !NIL_P(inclusive_end)) {
        req.inclusive_end = RTEST(inclusive_end);
      }
      if (VALUE reduce = rb_hash_aref(options, rb_id2sym(rb_intern("reduce"))); !NIL_P(reduce)) {
        req.reduce = RTEST(reduce);
      }
      if (VALUE group = rb_hash_aref(options, rb_id2sym(rb_intern("group"))); !NIL_P(group)) {
        req.group = RTEST(group);
      }
      if (VALUE group_level = rb_hash_aref(options, rb_id2sym(rb_intern("group_level")));
          !NIL_P(group_level)) {
        cb_check_type(group_level, T_FIXNUM);
        req.group_level = FIX2ULONG(group_level);
      }
      if (VALUE sort_order = rb_hash_aref(options, rb_id2sym(rb_intern("order")));
          !NIL_P(sort_order)) {
        cb_check_type(sort_order, T_SYMBOL);
        if (ID order = rb_sym2id(sort_order); order == rb_intern("ascending")) {
          req.order = core::view_sort_order::ascending;
        } else if (order == rb_intern("descending")) {
          req.order = core::view_sort_order::descending;
        }
      }
      if (VALUE keys = rb_hash_aref(options, rb_id2sym(rb_intern("keys"))); !NIL_P(keys)) {
        cb_check_type(keys, T_ARRAY);
        auto entries_num = static_cast<std::size_t>(RARRAY_LEN(keys));
        req.keys.reserve(entries_num);
        for (std::size_t i = 0; i < entries_num; ++i) {
          VALUE entry = rb_ary_entry(keys, static_cast<long>(i));
          cb_check_type(entry, T_STRING);
          req.keys.emplace_back(cb_string_new(entry));
        }
      }
    }

    auto promise = std::make_shared<std::promise<core::operations::document_view_response>>();
    auto f = promise->get_future();
    cluster->execute(req, [promise](auto&& resp) {
      promise->set_value(std::forward<decltype(resp)>(resp));
    });
    auto resp = cb_wait_for_future(f);
    if (resp.ctx.ec) {
      if (resp.error) {
        cb_throw_error(resp.ctx,
                       fmt::format(R"(unable to execute view query {} ({}))",
                                   resp.error->code,
                                   resp.error->message));
      } else {
        cb_throw_error(resp.ctx, "unable to execute view query");
      }
    }
    VALUE res = rb_hash_new();

    VALUE meta = rb_hash_new();
    if (resp.meta.total_rows) {
      rb_hash_aset(meta, rb_id2sym(rb_intern("total_rows")), ULL2NUM(*resp.meta.total_rows));
    }
    if (resp.meta.debug_info) {
      rb_hash_aset(
        meta, rb_id2sym(rb_intern("debug_info")), cb_str_new(resp.meta.debug_info.value()));
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("meta")), meta);

    VALUE rows = rb_ary_new_capa(static_cast<long>(resp.rows.size()));
    for (const auto& entry : resp.rows) {
      VALUE row = rb_hash_new();
      if (entry.id) {
        rb_hash_aset(row, rb_id2sym(rb_intern("id")), cb_str_new(entry.id.value()));
      }
      rb_hash_aset(row, rb_id2sym(rb_intern("key")), cb_str_new(entry.key));
      rb_hash_aset(row, rb_id2sym(rb_intern("value")), cb_str_new(entry.value));
      rb_ary_push(rows, row);
    }
    rb_hash_aset(res, rb_id2sym(rb_intern("rows")), rows);

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
init_views(VALUE cBackend)
{
  rb_define_method(cBackend, "document_view", cb_Backend_document_view, 5);

  rb_define_method(cBackend, "view_index_get_all", cb_Backend_view_index_get_all, 3);
  rb_define_method(cBackend, "view_index_get", cb_Backend_view_index_get, 4);
  rb_define_method(cBackend, "view_index_drop", cb_Backend_view_index_drop, 4);
  rb_define_method(cBackend, "view_index_upsert", cb_Backend_view_index_upsert, 4);
}
} // namespace couchbase::ruby
