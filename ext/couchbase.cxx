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

#include <ruby.h>

#include "rcb_analytics.hxx"
#include "rcb_backend.hxx"
#include "rcb_buckets.hxx"
#include "rcb_collections.hxx"
#include "rcb_crud.hxx"
#include "rcb_diagnostics.hxx"
#include "rcb_exceptions.hxx"
#include "rcb_extras.hxx"
#include "rcb_logger.hxx"
#include "rcb_multi.hxx"
#include "rcb_query.hxx"
#include "rcb_range_scan.hxx"
#include "rcb_search.hxx"
#include "rcb_users.hxx"
#include "rcb_version.hxx"
#include "rcb_views.hxx"

extern "C" {
#if defined(_WIN32)
__declspec(dllexport)
#endif
void
Init_libcouchbase(void)
{
  couchbase::ruby::install_terminate_handler();
  couchbase::ruby::init_logger();

  VALUE mCouchbase = rb_define_module("Couchbase");

  couchbase::ruby::init_version(mCouchbase);
  couchbase::ruby::init_exceptions(mCouchbase);

  VALUE cBackend = couchbase::ruby::init_backend(mCouchbase);

  couchbase::ruby::init_crud(cBackend);
  couchbase::ruby::init_multi(cBackend);
  couchbase::ruby::init_analytics(cBackend);
  couchbase::ruby::init_views(cBackend);
  couchbase::ruby::init_search(cBackend);
  couchbase::ruby::init_query(cBackend);
  couchbase::ruby::init_buckets(cBackend);
  couchbase::ruby::init_collections(cBackend);
  couchbase::ruby::init_users(cBackend);
  couchbase::ruby::init_range_scan(mCouchbase, cBackend);
  couchbase::ruby::init_diagnostics(cBackend);
  couchbase::ruby::init_extras(cBackend);
  couchbase::ruby::init_logger_methods(cBackend);
}
}
