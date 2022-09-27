#  Copyright 2020-2022 Couchbase, Inc.
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
  module ConfigProfiles
    class Profiles
      attr :profiles

      def initialize
        @profiles = {}
        register_profile("wan_development", DevelopmentProfile.new)
      end

      def register_profile(name, profile)
        @profiles[name] = profile
      end

      def apply(profile_name, options)
        raise ArgumentError, "#{profile_name} is not a registered profile" unless @profiles.key?(profile_name)

        @profiles[profile_name].apply(options)
      end
    end

    class Profile
      def apply(options); end
    end

    class DevelopmentProfile < Profile
      def apply(options)
        options.key_value_timeout = 20_000
        # TODO: Add `options.key_value_durable_timeout = 20_000` when key_value_durable_timeout is added to Options::Cluster
        options.connect_timeout = 20_000
        options.view_timeout = 120_000
        options.query_timeout = 120_000
        options.analytics_timeout = 120_000
        options.search_timeout = 120_000
        options.management_timeout = 120_000
      end
    end

    KNOWN_PROFILES = Profiles.new
  end
end
