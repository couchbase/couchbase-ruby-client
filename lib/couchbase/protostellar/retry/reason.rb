# frozen_string_literal: true

#  Copyright 2023-Present Couchbase, Inc.
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
  module Protostellar
    module Retry
      class Reason
        attr_reader :allows_non_idempotent_retry
        attr_reader :always_retry

        def initialize(allows_non_idempotent_retry, always_retry)
          @allows_non_idempotent_retry = allows_non_idempotent_retry
          @always_retry = always_retry
        end

        UNKNOWN = Reason.new(false, false).freeze
        SOCKET_NOT_AVAILABLE = Reason.new(true, false).freeze
        SERVICE_NOT_AVAILABLE = Reason.new(true, false).freeze
        NODE_NOT_AVAILABLE = Reason.new(true, false).freeze
        KV_NOT_MY_VBUCKET = Reason.new(true, true).freeze
        KV_COLLECTION_OUTDATED = Reason.new(true, true).freeze
        KV_ERROR_MAP_RETRY_INDICATED = Reason.new(true, false).freeze
        KV_LOCKED = Reason.new(true, false).freeze
        KV_TEMPORARY_FAILURE = Reason.new(true, false).freeze
        KV_SYNC_WRITE_IN_PROGRESS = Reason.new(true, false).freeze
        KV_SYNC_WRITE_RE_COMMIT_IN_PROGRESS = Reason.new(true, false).freeze
        SERVICE_RESPONSE_CODE_INDICATED = Reason.new(true, false).freeze
        SOCKET_CLOSED_WHILE_IN_FLIGHT = Reason.new(false, false).freeze
        CIRCUIT_BREAKER_OPEN = Reason.new(true, false).freeze
        QUERY_PREPARED_STATEMENT_FAILURE = Reason.new(true, false).freeze
        QUERY_INDEX_NOT_FOUND = Reason.new(true, false).freeze
        ANALYTICS_TEMPORARY_FAILURE = Reason.new(true, false).freeze
        SEARCH_TOO_MANY_REQUESTS = Reason.new(true, false).freeze
        VIEWS_TEMPORARY_FAILURE = Reason.new(true, false).freeze
        VIEWS_NO_ACTIVE_PARTITION = Reason.new(true, true).freeze
      end
    end
  end
end
