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

$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)

require "rubygems/version"
class ServerVersion
  def initialize(version_string)
    @version = Gem::Version.create(version_string)
  end

  def supports_gcccp?
    mad_hatter? || cheshire_cat?
  end

  def alice?
    @version >= Gem::Version.create("6.0.0") && @version < Gem::Version.create("6.5.0")
  end

  def mad_hatter?
    @version >= Gem::Version.create("6.5.0") && @version < Gem::Version.create("7.0.0")
  end

  def cheshire_cat?
    @version >= Gem::Version.create("7.0.0")
  end
end

require "couchbase"
module Couchbase
  TEST_CONNECTION_STRING = ENV["TEST_CONNECTION_STRING"] || "couchbase://localhost"
  TEST_USERNAME = ENV["TEST_USERNAME"] || "Administrator"
  TEST_PASSWORD = ENV["TEST_PASSWORD"] || "password"
  TEST_SERVER_VERSION = ServerVersion.new(ENV["TEST_SERVER_VERSION"] || "6.5.1")
end

require "minitest/autorun"

require "minitest/reporters"
Minitest::Reporters.use!(
    [
        Minitest::Reporters::SpecReporter.new,
        Minitest::Reporters::JUnitReporter.new
    ]
)
