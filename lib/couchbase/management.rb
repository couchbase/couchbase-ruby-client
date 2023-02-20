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

module Couchbase
  # This namespace includes manager classes to control cluster resources and perform maintenance operations.
  module Management
  end
end

require "couchbase/management/analytics_index_manager"
require "couchbase/management/bucket_manager"
require "couchbase/management/collection_manager"
require "couchbase/management/query_index_manager"
require "couchbase/management/collection_query_index_manager"
require "couchbase/management/search_index_manager"
require "couchbase/management/user_manager"
require "couchbase/management/view_index_manager"
