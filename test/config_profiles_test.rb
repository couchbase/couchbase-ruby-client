# frozen_string_literal: true

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

require_relative "test_helper"

module Couchbase
  class ConfigProfilesTest < Minitest::Test
    include TestUtilities

    class CustomProfile < Couchbase::ConfigProfiles::Profile
      def apply(options)
        options.key_value_timeout = 15000
        options.connect_timeout = 17000
      end
    end

    def setup
      # Do nothing
    end

    def teardown
      # Do nothing
    end

    def test_apply_development_config_profile
      options = Couchbase::Options::Cluster.new
      options.apply_profile("wan_development")

      assert_equal 20000, options.key_value_timeout
      assert_equal 20000, options.connect_timeout
      assert_equal 120000, options.view_timeout
      assert_equal 120000, options.query_timeout
      assert_equal 120000, options.analytics_timeout
      assert_equal 120000, options.search_timeout
      assert_equal 120000, options.management_timeout
    end

    def test_apply_custom_config_profile
      Couchbase::ConfigProfiles::KNOWN_PROFILES.register_profile("custom_profile", CustomProfile.new)
      options = Couchbase::Options::Cluster.new
      options.apply_profile("custom_profile")

      assert_equal 15000, options.key_value_timeout
      assert_equal 17000, options.connect_timeout
    end
  end
end
