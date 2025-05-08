#  Copyright 2023. Couchbase, Inc.
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

# frozen_string_literal: true

module Couchbase
  module Protostellar
    module Retry
      class Reason
        attr_reader :allows_non_idempotent_retry
        attr_reader :always_retry
        attr_reader :name

        def self.reason(name, allows_non_idempotent_retry:, always_retry:)
          const_set(
            name,
            new(name: name,
                allows_non_idempotent_retry: allows_non_idempotent_retry,
                always_retry: always_retry).freeze,
          )
        end

        def initialize(name:, allows_non_idempotent_retry:, always_retry:)
          @name = name
          @allows_non_idempotent_retry = allows_non_idempotent_retry
          @always_retry = always_retry
        end

        def to_s
          @name.downcase.to_s
        end

        reason :UNKNOWN, allows_non_idempotent_retry: false, always_retry: false
        reason :SOCKET_NOT_AVAILABLE, allows_non_idempotent_retry: true, always_retry: false
        reason :SERVICE_NOT_AVAILABLE, allows_non_idempotent_retry: true, always_retry: false
        reason :NODE_NOT_AVAILABLE, allows_non_idempotent_retry: true, always_retry: false
        reason :KV_NOT_MY_VBUCKET, allows_non_idempotent_retry: true, always_retry: true
        reason :KV_COLLECTION_OUTDATED, allows_non_idempotent_retry: true, always_retry: true
        reason :KV_ERROR_MAP_RETRY_INDICATED, allows_non_idempotent_retry: true, always_retry: false
        reason :KV_LOCKED, allows_non_idempotent_retry: true, always_retry: false
        reason :KV_TEMPORARY_FAILURE, allows_non_idempotent_retry: true, always_retry: false
        reason :KV_SYNC_WRITE_IN_PROGRESS, allows_non_idempotent_retry: true, always_retry: false
        reason :KV_SYNC_WRITE_RE_COMMIT_IN_PROGRESS, allows_non_idempotent_retry: true, always_retry: false
        reason :SERVICE_RESPONSE_CODE_INDICATED, allows_non_idempotent_retry: true, always_retry: false
        reason :SOCKET_CLOSED_WHILE_IN_FLIGHT, allows_non_idempotent_retry: false, always_retry: false
        reason :CIRCUIT_BREAKER_OPEN, allows_non_idempotent_retry: true, always_retry: false
        reason :QUERY_PREPARED_STATEMENT_FAILURE, allows_non_idempotent_retry: true, always_retry: false
        reason :QUERY_INDEX_NOT_FOUND, allows_non_idempotent_retry: true, always_retry: false
        reason :ANALYTICS_TEMPORARY_FAILURE, allows_non_idempotent_retry: true, always_retry: false
        reason :SEARCH_TOO_MANY_REQUESTS, allows_non_idempotent_retry: true, always_retry: false
        reason :VIEWS_TEMPORARY_FAILURE, allows_non_idempotent_retry: true, always_retry: false
        reason :VIEWS_NO_ACTIVE_PARTITION, allows_non_idempotent_retry: true, always_retry: true
      end
    end
  end
end
