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
          [:duration, time_or_duration.in_seconds]
        elsif time_or_duration.respond_to?(:tv_sec) # Time
          [:time_point, time_or_duration.tv_sec]
        elsif time_or_duration.is_a?(Integer)
          if time_or_duration < RELATIVE_EXPIRY_CUTOFF_SECONDS
            # looks like valid relative duration as specified in protocol (less than 30 days)
            [:duration, time_or_duration]
          elsif time_or_duration > WORKAROUND_EXPIRY_CUTOFF_SECONDS
            effective_expiry = ::Time.at(time_or_duration).utc
            warn "The specified expiry duration #{time_or_duration} is longer than 50 years. For bug-compatibility " \
                 "with previous versions of SDK 3.0.x, the number of seconds in the duration will be interpreted as " \
                 "the epoch second when the document should expire (#{effective_expiry}). Stuffing an epoch second " \
                 "into a Duration is deprecated and will no longer work in SDK 3.1. Consider using Time instance instead."
            [:time_point, time_or_duration]
          else
            [:time_point, ::Time.now.tv_sec + time_or_duration]
          end
        else
          [:duration, time_or_duration]
        end
      end

      # This method converts its argument to milliseconds
      #
      # 1. Integer values are interpreted as a number of milliseconds
      # 2. If the argument is a Duration-like object and responds to #in_milliseconds,
      #    then use it and convert result to Integer
      # 3. Otherwise invoke #to_i on the argument and interpret it as a number of milliseconds
      def extract_duration(number_or_duration)
        return unless number_or_duration
        return number_or_duration if number_or_duration.class == Integer # rubocop:disable Style/ClassEqualityComparison avoid overrides of #is_a?, #kind_of?

        if number_or_duration.respond_to?(:in_milliseconds)
          number_or_duration.public_send(:in_milliseconds)
        else
          number_or_duration
        end.to_i
      end
    end
  end
end
