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
    class GetResult
      attr_reader :encoded
      attr_reader :content_type
      attr_reader :compression_type
      attr_reader :cas
      attr_reader :expiry
      attr_reader :transcoder
      attr_accessor :flags

      def success?
        cas != 0
      end

      def initialize(resp)
        @encoded = resp.content
        @content_type = resp.content_type.downcase
        @compression_type = resp.compression_type.downcase
        @cas = resp.cas
        @expiry = resp.expiry
        if @content_type == :json
          @transcoder = JsonTranscoder.new
        end
      end

      def content(transcoder = self.transcoder)
        transcoder ? transcoder.decode(@encoded) : @encoded
      end
    end
  end
end
