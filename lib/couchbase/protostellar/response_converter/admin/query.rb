# frozen_string_literal: true

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

require "couchbase/management/query_index_manager"

module Couchbase
  module Protostellar
    module ResponseConverter
      module Admin
        class Query
          INDEX_TYPE_MAP = {
            :INDEX_TYPE_VIEW => :view,
            :INDEX_TYPE_GSI => :gsi,
          }.freeze

          INDEX_STATE_MAP = {
            :INDEX_STATE_DEFERRED => :deferred,
            :INDEX_STATE_BUILDING => :building,
            :INDEX_STATE_PENDING => :pending,
            :INDEX_STATE_ONLINE => :online,
            :INDEX_STATE_OFFLINE => :offline,
            :INDEX_STATE_ABRIDGED => :abridged,
            :INDEX_STATE_SCHEDULED => :scheduled,
          }.freeze

          def self.to_query_index_array(resp)
            resp.indexes.map do |proto_idx|
              Couchbase::Management::QueryIndex.new do |idx|
                idx.bucket = proto_idx.bucket_name
                idx.scope = proto_idx.scope_name
                idx.collection = proto_idx.collection_name
                idx.name = proto_idx.name
                idx.is_primary = proto_idx.is_primary
                idx.type = INDEX_TYPE_MAP[proto_idx.type]
                idx.state = INDEX_STATE_MAP[proto_idx.state]
                idx.index_key = proto_idx.fields.to_a
                idx.condition = proto_idx.condition if proto_idx.has_condition?
                idx.partition = proto_idx.partition if proto_idx.has_partition?
              end
            end
          end
        end
      end
    end
  end
end
