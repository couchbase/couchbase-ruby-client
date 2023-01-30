# frozen_string_literal: true

#  Copyright 2022-Present. Couchbase, Inc.
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
  module StellarNebula
    class RemoveOptions
      attr_accessor :cas
      attr_accessor :durability_level
      attr_reader :timeout

      def initialize(cas: nil,
                     durability_level: :none,
                     timeout: nil)
        @cas = cas
        @durability_level = durability_level
        @timeout = timeout
      end

      def to_request
        opts = {}
        opts[:durability_level] = @durability_level.upcase unless @durability_level == :none
        opts[:cas] = @cas unless @cas.nil?
        opts
      end

      DEFAULT = RemoveOptions.new.freeze
    end
  end
end
