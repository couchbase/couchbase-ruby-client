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

library_directory = File.expand_path("../lib", __dir__)
if File.directory?(library_directory)
  $LOAD_PATH.unshift(library_directory)
else
  warn("using system library path")
end

begin
  require "simplecov-cobertura"
  SimpleCov.formatter = SimpleCov::Formatter::CoberturaFormatter
  SimpleCov.start
rescue LoadError
  warn("running tests without coverage")
end

require "rubygems/version"

class ServerVersion
  def initialize(version_string, developer_preview: false)
    @version = Gem::Version.create(version_string)
    @developer_preview = developer_preview
  end

  def to_s
    "version=#{@version}, dev=#{@developer_preview}"
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

  def supports_collections?
    (mad_hatter? && @developer_preview) || cheshire_cat?
  end

  def supports_sync_replication?
    mad_hatter? || cheshire_cat?
  end

  def supports_scoped_queries?
    cheshire_cat?
  end
end

require "couchbase"
require "json"

require "minitest/autorun"

module Couchbase
  TEST_CONNECTION_STRING = ENV["TEST_CONNECTION_STRING"] || "couchbase://127.0.0.1"
  TEST_USERNAME = ENV["TEST_USERNAME"] || "Administrator"
  TEST_PASSWORD = ENV["TEST_PASSWORD"] || "password"
  TEST_BUCKET = ENV["TEST_BUCKET"] || "default"
  TEST_DEVELOPER_PREVIEW = ENV.key?("TEST_DEVELOPER_PREVIEW")
  TEST_SERVER_VERSION = ServerVersion.new(ENV["TEST_SERVER_VERSION"] || "6.5.1", developer_preview: TEST_DEVELOPER_PREVIEW)

  class BaseTest < Minitest::Test
    # rubocop:disable Minitest/TestMethodName

    def uniq_id(name)
      parent = caller_locations&.first
      prefix = "#{File.basename(parent&.path, '.rb')}_#{parent&.lineno}"
      "#{prefix}_#{name}_#{Time.now.to_f}"
    end

    def load_raw_test_dataset(dataset)
      File.read(File.join(__dir__, "..", "test_data", "#{dataset}.json"))
    end

    def load_json_test_dataset(dataset)
      JSON.parse(load_raw_test_dataset(dataset))
    end

    # rubocop:enable Minitest/TestMethodName
  end
end

require "minitest/reporters"
Minitest::Reporters.use!(
  [
    Minitest::Reporters::SpecReporter.new,
    Minitest::Reporters::JUnitReporter.new,
  ]
)
