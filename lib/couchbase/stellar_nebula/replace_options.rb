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
    class ReplaceOptions
      attr_accessor :expiry
      attr_accessor :transcoder
      attr_accessor :cas
      attr_accessor :durability_level
      attr_accessor :preserve_expiry
      attr_reader :timeout
      attr_reader :document_content_type

      def initialize(expiry: nil,
                     preserve_expiry: false,
                     transcoder: JsonTranscoder.new,
                     cas: nil,
                     durability_level: :none,
                     timeout: nil)
        @timeout = timeout
        @transcoder = transcoder
        @document_content_type = transcoder.respond_to?(:document_content_type) ? transcoder.document_content_type : :unknown
        @durability_level = durability_level
        @cas = cas

        raise ArgumentError, "`preserve_expiry` and `expiry` cannot both be set at the same time" unless expiry.nil? || !preserve_expiry

        @expiry = expiry
        @preserve_expiry = preserve_expiry

        yield self if block_given?
      end

      def to_request
        opts = {
          content_type: @document_content_type.upcase,
        }
        opts[:durability_level] = @durability_level.upcase unless @durability_level == :none
        opts[:cas] = @cas unless @cas.nil?

        unless @preserve_expiry
          timestamp_secs = @expiry.nil? ? 0 : Utils::Time.extract_expiry_time_point(@expiry)
          opts[:expiry] = Google::Protobuf::Timestamp.new({:seconds => timestamp_secs})
        end
        opts
      end

      DEFAULT = ReplaceOptions.new.freeze
    end
  end
end
