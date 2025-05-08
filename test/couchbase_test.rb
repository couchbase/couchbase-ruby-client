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

require_relative "test_helper"

require "tempfile"

class CouchbaseTest < Minitest::Test
  include Couchbase::TestUtilities

  def test_that_it_has_a_version_number
    refute_nil ::Couchbase::VERSION[:sdk]
    refute_nil ::Couchbase::BUILD_INFO[:cxx_client][:version]
  end

  def test_it_can_use_configuration_with_connect
    Tempfile.open('couchbase.yaml') do |temp_file|
      temp_file.write(YAML.dump(
                        {
                          'test' => {
                            'connection_string' => env.connection_string,
                            'username' => env.username,
                            'password' => env.password,
                          },
                        },
                      ))
      temp_file.flush

      ENV["COUCHBASE_ENV"] = "test"
      config = Couchbase::Configuration.new
      config.load!(temp_file.path)

      cluster = Couchbase::Cluster.connect(config)

      refute_nil cluster
      cluster.disconnect
    end
  end
end
