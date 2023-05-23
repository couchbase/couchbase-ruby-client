# frozen_string_literal: true

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
