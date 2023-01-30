# frozen_string_literal: true

#  Copyright 2022-Present Couchbase, Inc.
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

require_relative "json_transcoder"

module Couchbase
  module StellarNebula
    class GetOptions
      attr_accessor :with_expiry
      attr_accessor :transcoder
      attr_accessor :preserve_array_indexes
      attr_accessor :projections
      attr_accessor :timeout

      def initialize(projections: [],
                     with_expiry: false,
                     transcoder: JsonTranscoder.new,
                     timeout: nil)
        @projections = projections
        @with_expiry = with_expiry
        @transcoder = transcoder
        @preserve_array_indexes = false
        @timeout = timeout
      end


      DEFAULT = GetOptions.new.freeze

      def project(*paths)
        @projections ||= []
        @projections |= paths.flatten # union with current projections
      end

      def need_projected_get?
        @with_expiry || !@projections.empty?
      end

      def to_request
        {}
      end
    end
  end
end
