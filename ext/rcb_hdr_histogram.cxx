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

#include "rcb_hdr_histogram.hxx"
#include "rcb_exceptions.hxx"

#include <hdr/hdr_histogram.h>
#include <ruby.h>

#include <mutex>
#include <shared_mutex>
#include <vector>

namespace couchbase::ruby
{
namespace
{
struct cb_hdr_histogram_data {
  hdr_histogram* histogram{ nullptr };
  std::shared_mutex mutex{};
};

void
cb_hdr_histogram_close(cb_hdr_histogram_data* hdr_histogram_data)
{
  if (hdr_histogram_data->histogram != nullptr) {
    hdr_close(hdr_histogram_data->histogram);
    hdr_histogram_data->histogram = nullptr;
  }
}

void
cb_HdrHistogramC_mark(void* /* ptr */)
{
  /* no embedded ruby objects -- no mark */
}

void
cb_HdrHistogramC_free(void* ptr)
{
  auto* hdr_histogram_data = static_cast<cb_hdr_histogram_data*>(ptr);
  cb_hdr_histogram_close(hdr_histogram_data);
  hdr_histogram_data->~cb_hdr_histogram_data();
  ruby_xfree(hdr_histogram_data);
}

std::size_t
cb_HdrHistogramC_memsize(const void* ptr)
{
  const auto* hdr_histogram_data = static_cast<const cb_hdr_histogram_data*>(ptr);
  return sizeof(*hdr_histogram_data);
}

const rb_data_type_t cb_hdr_histogram_type{
  "Couchbase/Utils/HdrHistogramC",
  {
    cb_HdrHistogramC_mark,
    cb_HdrHistogramC_free,
    cb_HdrHistogramC_memsize,
// only one reserved field when GC.compact implemented
#ifdef T_MOVED
    nullptr,
#endif
    {},
  },
#ifdef RUBY_TYPED_FREE_IMMEDIATELY
  nullptr,
  nullptr,
  RUBY_TYPED_FREE_IMMEDIATELY,
#endif
};

VALUE
cb_HdrHistogramC_allocate(VALUE klass)
{
  cb_hdr_histogram_data* hdr_histogram = nullptr;
  VALUE obj =
    TypedData_Make_Struct(klass, cb_hdr_histogram_data, &cb_hdr_histogram_type, hdr_histogram);
  new (hdr_histogram) cb_hdr_histogram_data();
  return obj;
}

VALUE
cb_HdrHistogramC_initialize(VALUE self,
                            VALUE lowest_discernible_value,
                            VALUE highest_trackable_value,
                            VALUE significant_figures)
{
  Check_Type(lowest_discernible_value, T_FIXNUM);
  Check_Type(highest_trackable_value, T_FIXNUM);
  Check_Type(significant_figures, T_FIXNUM);

  std::int64_t lowest = NUM2LL(lowest_discernible_value);
  std::int64_t highest = NUM2LL(highest_trackable_value);
  int sigfigs = NUM2INT(significant_figures);

  cb_hdr_histogram_data* hdr_histogram;
  TypedData_Get_Struct(self, cb_hdr_histogram_data, &cb_hdr_histogram_type, hdr_histogram);

  int res;
  {
    const std::unique_lock lock(hdr_histogram->mutex);
    res = hdr_init(lowest, highest, sigfigs, &hdr_histogram->histogram);
  }
  if (res != 0) {
    rb_raise(exc_couchbase_error(), "failed to initialize HDR histogram");
    return self;
  }

  return self;
}

VALUE
cb_HdrHistogramC_close(VALUE self)
{
  cb_hdr_histogram_data* hdr_histogram;
  TypedData_Get_Struct(self, cb_hdr_histogram_data, &cb_hdr_histogram_type, hdr_histogram);
  {
    const std::unique_lock lock(hdr_histogram->mutex);
    cb_hdr_histogram_close(hdr_histogram);
  }
  return Qnil;
}

VALUE
cb_HdrHistogramC_record_value(VALUE self, VALUE value)
{
  Check_Type(value, T_FIXNUM);

  std::int64_t val = NUM2LL(value);

  cb_hdr_histogram_data* hdr_histogram;
  TypedData_Get_Struct(self, cb_hdr_histogram_data, &cb_hdr_histogram_type, hdr_histogram);

  {
    const std::shared_lock lock(hdr_histogram->mutex);
    hdr_record_value_atomic(hdr_histogram->histogram, val);
  }
  return Qnil;
}

VALUE
cb_HdrHistogramC_get_percentiles_and_reset(VALUE self, VALUE percentiles)
{
  Check_Type(percentiles, T_ARRAY);

  cb_hdr_histogram_data* hdr_histogram;
  TypedData_Get_Struct(self, cb_hdr_histogram_data, &cb_hdr_histogram_type, hdr_histogram);

  std::vector<std::int64_t> percentile_values{};
  std::int64_t total_count;
  {
    const std::unique_lock lock(hdr_histogram->mutex);
    total_count = hdr_histogram->histogram->total_count;
    for (std::size_t i = 0; i < static_cast<std::size_t>(RARRAY_LEN(percentiles)); ++i) {
      VALUE entry = rb_ary_entry(percentiles, static_cast<long>(i));
      Check_Type(entry, T_FLOAT);
      double perc = NUM2DBL(entry);
      std::int64_t value_at_perc = hdr_value_at_percentile(hdr_histogram->histogram, perc);
      percentile_values.push_back(value_at_perc);
    }
    hdr_reset(hdr_histogram->histogram);
  }

  static const VALUE sym_total_count = rb_id2sym(rb_intern("total_count"));
  static const VALUE sym_percentiles = rb_id2sym(rb_intern("percentiles"));
  VALUE res = rb_hash_new();
  rb_hash_aset(res, sym_total_count, LL2NUM(total_count));
  VALUE perc_array = rb_ary_new_capa(static_cast<long>(percentile_values.size()));
  for (const auto& val : percentile_values) {
    rb_ary_push(perc_array, LL2NUM(val));
  }
  rb_hash_aset(res, sym_percentiles, perc_array);
  return res;
}

VALUE
cb_HdrHistogramC_bin_count(VALUE self)
{
  cb_hdr_histogram_data* hdr_histogram;
  TypedData_Get_Struct(self, cb_hdr_histogram_data, &cb_hdr_histogram_type, hdr_histogram);

  std::int32_t bin_count;
  {
    const std::unique_lock lock(hdr_histogram->mutex);
    bin_count = hdr_histogram->histogram->bucket_count;
  }
  return LONG2NUM(bin_count);
}
} // namespace

void
init_hdr_histogram(VALUE mCouchbase)
{
  VALUE mUtils = rb_define_module_under(mCouchbase, "Utils");
  VALUE cHdrHistogramC = rb_define_class_under(mUtils, "HdrHistogramC", rb_cObject);
  rb_define_alloc_func(cHdrHistogramC, cb_HdrHistogramC_allocate);
  rb_define_method(cHdrHistogramC, "initialize", cb_HdrHistogramC_initialize, 3);
  rb_define_method(cHdrHistogramC, "close", cb_HdrHistogramC_close, 0);
  rb_define_method(cHdrHistogramC, "record_value", cb_HdrHistogramC_record_value, 1);
  rb_define_method(cHdrHistogramC, "bin_count", cb_HdrHistogramC_bin_count, 0);
  rb_define_method(
    cHdrHistogramC, "get_percentiles_and_reset", cb_HdrHistogramC_get_percentiles_and_reset, 1);
}
} // namespace couchbase::ruby
