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

module Couchbase
  # @api private
  class TranscoderFlags
    FORMAT_MAP = {
      reserved: 0,
      private: 1,
      json: 2,
      binary: 3,
      string: 4,
    }.freeze
    INV_FORMAT_MAP = FORMAT_MAP.invert.freeze

    COMPRESSION_MAP = {none: 0}.freeze
    INV_COMPRESSION_MAP = COMPRESSION_MAP.invert

    attr_reader :format
    attr_reader :compression
    attr_reader :lower_bits

    def initialize(format:, compression: :none, lower_bits: 0)
      @format = format
      @compression = compression
      @lower_bits = lower_bits
    end

    def self.decode(flags)
      return TranscoderFlags.new(format: flags) if flags.is_a?(Symbol)

      common_flags = flags >> 24
      lower_bits = flags & 0x00ffff

      return TranscoderFlags.new(format: nil, lower_bits: lower_bits) if common_flags.zero?

      compression_bits = common_flags >> 5
      format_bits = common_flags & 0x0f
      TranscoderFlags.new(
        format: INV_FORMAT_MAP[format_bits],
        compression: INV_COMPRESSION_MAP[compression_bits],
        lower_bits: lower_bits
      )
    end

    def encode
      common_flags = (COMPRESSION_MAP[@compression] << 5) | FORMAT_MAP[@format]
      (common_flags << 24) | @lower_bits
    end
  end
end
