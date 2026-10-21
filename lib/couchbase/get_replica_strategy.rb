# frozen_string_literal: true

#  Copyright 2026. Couchbase, Inc.
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

require "couchbase/errors"

module Couchbase
  # Selects the replica that {Collection#get_replica} reads from.
  #
  # @since 3.9.0
  class GetReplicaStrategy
    REPLICA_INDEXES = {first: 0, second: 1, third: 2}.freeze
    private_constant :REPLICA_INDEXES

    # @return [Symbol] the requested replica, one of +:first+, +:second+ or +:third+
    attr_reader :index

    # @return [Boolean] whether an index that cannot be read resolves to the next replica that can
    attr_reader :wrap

    private_class_method :new

    # Selects the replica at the given index.
    #
    # Without +wrap+, the SDK resolves the index against the vbucket map it already holds. No network round trip
    # is made for these two errors:
    # * an index at or beyond the bucket's configured replica count raises {Error::ReplicaIndexOutOfBounds}
    # * a configured replica the vbucket map places on no node raises {Error::ReplicaIndexCurrentlyUnavailable}
    #
    # The replica is chosen from the vbucket map, not from node health. A replica on a node that is down is still
    # chosen. The read can then wait for that node until the read times out.
    #
    # @param [Symbol] index the replica to read from, one of +:first+, +:second+ or +:third+
    # @param [Boolean] wrap if +true+, an unreadable index moves to the next readable replica. The walk starts at the
    #   index modulo the configured replica count. A bucket configured without replicas still raises
    #   {Error::ReplicaIndexOutOfBounds}. If no replica is readable, it still raises
    #   {Error::ReplicaIndexCurrentlyUnavailable}.
    #
    # @raise [Error::InvalidArgument] if +index+ is not one of the supported symbols, or +wrap+ is not a Boolean
    #
    # @example Read the first replica, or the next readable one
    #   collection.get_replica("airline_10", GetReplicaStrategy.from_index(:first, wrap: true))
    #
    # @example Bound the wait for a replica whose node is down, then read any copy
    #   begin
    #     collection.get_replica("airline_10", GetReplicaStrategy.from_index(:first, wrap: true),
    #                            Options::GetReplica(timeout: 500))
    #   rescue Error::UnambiguousTimeout
    #     collection.get_any_replica("airline_10")
    #   end
    #
    # @return [GetReplicaStrategy]
    def self.from_index(index, wrap: false)
      unless REPLICA_INDEXES.key?(index)
        raise Error::InvalidArgument, "replica index must be one of #{REPLICA_INDEXES.keys.inspect}, but given #{index.inspect}"
      end
      raise Error::InvalidArgument, "wrap must be a Boolean, but given #{wrap.inspect}" unless [true, false].include?(wrap)

      new(index, wrap)
    end

    # @api private
    def initialize(index, wrap)
      @index = index
      @wrap = wrap
      freeze
    end

    # @api private
    def to_backend
      {
        replica_index: REPLICA_INDEXES.fetch(@index),
        wrap: @wrap,
      }
    end
  end
end
