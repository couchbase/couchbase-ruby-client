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

require "couchbase/transcoder_flags"
require "couchbase/errors"

module Couchbase
  class RawStringTranscoder
    # @param [String] document
    # @return [Array<String, Integer>] pair of encoded document and flags
    def encode(document)
      unless document.is_a?(String) && document.valid_encoding?
        raise Error::EncodingFailure, "Only String data supported by RawStringTranscoder"
      end

      [document, TranscoderFlags.new(format: :string).encode]
    end

    # @param [String] blob string of bytes, containing encoded representation of the document
    # @param [Integer] flags bit field, describing how the data encoded
    # @return [String] decoded document
    def decode(blob, flags)
      format = TranscoderFlags.decode(flags).format
      raise Error::DecodingFailure, "Unable to decode #{format} with the RawStringTranscoder" unless format == :string || format.nil?

      blob
    end
  end
end
