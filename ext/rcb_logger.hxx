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

#ifndef COUCHBASE_RUBY_RCB_LOGGER_HXX
#define COUCHBASE_RUBY_RCB_LOGGER_HXX

#include <ruby/internal/value.h>

namespace couchbase::ruby
{
void
install_terminate_handler();

void
init_logger();

void
flush_logger();

void
init_logger_methods(VALUE cBackend);

} // namespace couchbase::ruby

#endif
