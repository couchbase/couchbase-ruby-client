require "google/protobuf/well_known_types"

module Couchbase
  module StellarNebula
    module Utils
      module Time
        RELATIVE_EXPIRY_CUTOFF_SECONDS = 30 * 24 * 60 * 60
        WORKAROUND_EXPIRY_CUTOFF_SECONDS = 50 * 365 * 24 * 60 * 60

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

        def extract_expiry_time_point(time_or_duration)
          type, t_or_d = extract_expiry_time(time_or_duration)
          if t_or_d.nil?
            nil
          elsif type == :duration
            ::Time.now.tv_sec + t_or_d
          else
            t_or_d
          end
        end

        def extract_duration(number_or_duration)
          number_or_duration.respond_to?(:in_milliseconds) ? number_or_duration.public_send(:in_milliseconds) : number_or_duration
        end
      end
    end
  end
end
