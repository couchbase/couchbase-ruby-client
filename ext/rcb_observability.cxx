/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025-Present Couchbase, Inc.
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

#include "rcb_utils.hxx"

#include <core/tracing/wrapper_sdk_tracer.hxx>

#include <ruby.h>

#include <chrono>
#include <memory>

namespace couchbase::ruby
{
void
cb_add_core_spans(VALUE observability_handler,
                  std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> parent_span,
                  std::size_t retry_attempts)
{
  const auto children = parent_span->children();
  VALUE spans = rb_ary_new_capa(static_cast<long>(children.size()));

  for (const auto& child : parent_span->children()) {
    VALUE span = rb_hash_new();

    static VALUE sym_name = rb_id2sym(rb_intern("name"));
    static VALUE sym_attributes = rb_id2sym(rb_intern("attributes"));
    static VALUE sym_start_timestamp = rb_id2sym(rb_intern("start_timestamp"));
    static VALUE sym_end_timestamp = rb_id2sym(rb_intern("end_timestamp"));

    VALUE attributes = rb_hash_new();

    for (const auto& [key, value] : child->uint_tags()) {
      rb_hash_aset(attributes, cb_str_new(key), ULL2NUM(value));
    }

    for (const auto& [key, value] : child->string_tags()) {
      rb_hash_aset(attributes, cb_str_new(key), cb_str_new(value));
    }

    rb_hash_aset(span, sym_name, cb_str_new(child->name()));
    rb_hash_aset(span, sym_attributes, attributes);
    rb_hash_aset(span,
                 sym_start_timestamp,
                 LL2NUM(std::chrono::duration_cast<std::chrono::microseconds>(
                          child->start_time().time_since_epoch())
                          .count()));
    rb_hash_aset(span,
                 sym_end_timestamp,
                 LL2NUM(std::chrono::duration_cast<std::chrono::microseconds>(
                          child->end_time().time_since_epoch())
                          .count()));

    rb_ary_push(spans, span);
  }

  static ID add_backend_spans_func = rb_intern("add_spans_from_backend");
  rb_funcall(observability_handler, add_backend_spans_func, 1, spans);

  if (retry_attempts > 0) {
    static ID add_retries_func = rb_intern("add_retries");
    rb_funcall(observability_handler, add_retries_func, ULONG2NUM(retry_attempts));
  }
}
} // namespace couchbase::ruby
