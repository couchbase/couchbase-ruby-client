#    Copyright 2020 Couchbase, Inc.
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

require "time"

module Couchbase
  module Utils
    # Various Time utilities
    module Time
      RELATIVE_EXPIRY_CUTOFF_SECONDS = 30 * 24 * 60 * 60
      WORKAROUND_EXPIRY_CUTOFF_SECONDS = 50 * 365 * 24 * 60 * 60

      module_function

      # @param [Integer, #in_seconds, Time, nil] time_or_duration expiration time to associate with the document
      def extract_expiry_time(time_or_duration)
        if time_or_duration.respond_to?(:in_seconds) # Duration
          time_or_duration.in_seconds
        elsif time_or_duration.respond_to?(:tv_sec) # Time
          time_or_duration.tv_sec
        elsif time_or_duration.is_a?(Integer)
          if time_or_duration < RELATIVE_EXPIRY_CUTOFF_SECONDS
            # looks like valid relative duration as specified in protocol (less than 30 days)
            time_or_duration
          elsif time_or_duration > WORKAROUND_EXPIRY_CUTOFF_SECONDS
            effective_expiry = ::Time.at(time_or_duration).utc
            warn "The specified expiry duration #{time_or_duration} is longer than 50 years. For bug-compatibility " \
                 "with previous versions of SDK 3.0.x, the number of seconds in the duration will be interpreted as " \
                 "the epoch second when the document should expire (#{effective_expiry}). Stuffing an epoch second " \
                 "into a Duration is deprecated and will no longer work in SDK 3.1. Consider using Time instance instead."
            time_or_duration
          else
            ::Time.now.tv_sec + time_or_duration
          end
        else
          time_or_duration
        end
      end
    end
  end
end
