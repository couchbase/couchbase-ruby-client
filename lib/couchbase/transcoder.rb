# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2013-2017 Couchbase, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'multi_json'
require 'ext/multi_json_fix'

module Couchbase
  module Transcoder
    module Compat
      @disabled = false

      def self.enable!
        @disabled = false
      end

      def self.disable!
        @disabled = true
      end

      def self.enabled?
        !@disabled
      end

      def self.guess_and_load(blob, flags, _options = {})
        case flags & Bucket::FMT_MASK
        when Bucket::FMT_DOCUMENT
          MultiJson.load(blob)
        when Bucket::FMT_MARSHAL
          ::Marshal.load(blob)
        when Bucket::FMT_PLAIN
          blob
        else
          raise ArgumentError, format('unexpected flags (0x%02x)', flags)
        end
      end
    end

    module Document
      def self.dump(obj, flags, _options = {})
        [
          MultiJson.dump(obj),
          (flags & ~Bucket::FMT_MASK) | Bucket::FMT_DOCUMENT
        ]
      end

      def self.load(blob, flags, options = {})
        if (flags & Bucket::FMT_MASK) == Bucket::FMT_DOCUMENT || options[:forced]
          MultiJson.load(blob)
        else
          return Compat.guess_and_load(blob, flags, options) if Compat.enabled?
          raise ArgumentError,
                format('unexpected flags (0x%02x instead of 0x%02x)', flags, Bucket::FMT_DOCUMENT)
        end
      end
    end

    module Marshal
      def self.dump(obj, flags, _options = {})
        [
          ::Marshal.dump(obj),
          (flags & ~Bucket::FMT_MASK) | Bucket::FMT_MARSHAL
        ]
      end

      def self.load(blob, flags, options = {})
        if (flags & Bucket::FMT_MASK) == Bucket::FMT_MARSHAL || options[:forced]
          ::Marshal.load(blob)
        else
          if Compat.enabled?
            Compat.guess_and_load(blob, flags, options)
          else
            raise ArgumentError,
                  format('unexpected flags (0x%02x instead of 0x%02x)', flags, Bucket::FMT_MARSHAL)
          end
        end
      end
    end

    module Plain
      def self.dump(obj, flags, _options = {})
        [
          obj,
          (flags & ~Bucket::FMT_MASK) | Bucket::FMT_PLAIN
        ]
      end

      def self.load(blob, flags, options = {})
        if (flags & Bucket::FMT_MASK) == Bucket::FMT_PLAIN || options[:forced]
          blob
        else
          if Compat.enabled?
            Compat.guess_and_load(blob, flags, options)
          else
            raise ArgumentError,
                  format('unexpected flags (0x%02x instead of 0x%02x)', flags, Bucket::FMT_PLAIN)
          end
        end
      end
    end
  end
end
