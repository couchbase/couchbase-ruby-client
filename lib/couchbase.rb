# frozen_string_literal: true

#  Copyright 2020-2021 Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

require "couchbase/version"
require "couchbase/libcouchbase"
require "couchbase/logger"
require "couchbase/cluster"

require "couchbase/railtie" if defined?(Rails)

# @!macro uncommitted
#   @couchbase.stability
#     Uncommitted: This API may change in the future.
#
# @!macro volatile
#   @couchbase.stability
#     Volatile: This API is subject to change at any time.
