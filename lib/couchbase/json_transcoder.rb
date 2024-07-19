# frozen_string_literal: true

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

require "json"

require "couchbase/transcoder_flags"

module Couchbase
  class JsonTranscoder
    # @param [Object] document
    # @return [Array<String, Integer>] pair of encoded document and flags
    def encode(document)
      raise Error::EncodingFailure, "The JsonTranscoder does not support binary data" if document.is_a?(String) && !document.valid_encoding?

      [JSON.generate(document), TranscoderFlags.new(format: :json, lower_bits: 6).encode]
    end

    # @param [String] blob string of bytes, containing encoded representation of the document
    # @param [Integer, :json] flags bit field, describing how the data encoded
    # @return [Object] decoded document
    def decode(blob, flags)
      format = TranscoderFlags.decode(flags).format
      raise Error::DecodingFailure, "Unable to decode #{format} with the JsonTranscoder" unless format == :json || format.nil?

      JSON.parse(blob) unless blob&.empty?
    end
  end
end
