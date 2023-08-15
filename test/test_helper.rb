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
    @version = Gem::Version.create(version_string.sub(/-\w+$/, ""))
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

  def elixir?
    @version >= Gem::Version.create("7.5.0")
  end

  def trinity?
    @version >= Gem::Version.create("7.6.0")
  end

  def supports_collections?
    cheshire_cat?
  end

  def supports_sync_replication?
    mad_hatter? || cheshire_cat?
  end

  def supports_scoped_queries?
    cheshire_cat?
  end

  def supports_create_as_deleted?
    cheshire_cat? || @version >= Gem::Version.create("6.6.0")
  end

  def supports_regexp_matches?
    mad_hatter? || cheshire_cat?
  end

  def supports_preserve_expiry?
    cheshire_cat?
  end

  def is_rcbc_408_applicable?
    @version < Gem::Version.create("7.0.0")
  end

  def supports_range_scan?
    elixir?
  end

  def supports_subdoc_read_from_replica?
    elixir?
  end
end

require "couchbase"
require "json"
require "securerandom"

require "minitest/autorun"

require_relative "mock_helper"

module Couchbase
  class TestEnvironment
    attr_writer :connection_string

    def connection_string
      @connection_string ||= ENV.fetch("TEST_CONNECTION_STRING", nil)
    end

    def want_caves?
      connection_string.nil? || connection_string.empty?
    end

    def username
      @username ||= ENV.fetch("TEST_USERNAME", nil) || "Administrator"
    end

    def password
      @password ||= ENV.fetch("TEST_PASSWORD", nil) || "password"
    end

    def bucket
      @bucket ||= ENV.fetch("TEST_BUCKET", nil) || "default"
    end

    def jenkins?
      ENV.key?("JENKINS_HOME")
    end

    def developer_preview?
      unless defined?(@developer_preview)
        @developer_preview = ENV.key?("TEST_DEVELOPER_PREVIEW") && ENV.fetch("TEST_DEVELOPER_PREVIEW",
                                                                             nil) == "yes"
      end
      @developer_preview
    end

    def server_version
      @server_version ||= ServerVersion.new(ENV.fetch("TEST_SERVER_VERSION", nil) || "6.6.0", developer_preview: developer_preview?)
    end
  end

  module TestUtilities
    def env
      @env ||= begin
        e = TestEnvironment.new
        if e.want_caves?
          @caves = Caves.new
          @caves.start
          @cluster_id = SecureRandom.uuid
          e.connection_string = @caves.create_cluster(@cluster_id)
          # FIXME: CAVES does not support Snappy
          e.connection_string += "?enable_compression=false"
        end
        e
      end
    end

    def use_caves?
      defined? @caves
    end

    def use_server?
      !use_caves?
    end

    def connect(options = Cluster::ClusterOptions.new)
      options.authenticate(env.username, env.password)
      @cluster = Cluster.connect(env.connection_string, options)
    end

    def disconnect
      @cluster.disconnect if defined? @cluster
    end

    # @param [Float] duration in seconds with fractions
    def time_travel(duration)
      if use_caves?
        @caves.time_travel_cluster(@cluster_id, (duration * 1_000).to_i)
      else
        sleep(duration)
      end
    end

    def uniq_id(name)
      parent = caller_locations&.first
      prefix = "#{File.basename(parent&.path, '.rb')}_#{parent&.lineno}"
      "#{prefix}_#{name}_#{Time.now.to_f.to_s.reverse}".sub(".", "-")
    end

    def load_raw_test_dataset(dataset)
      File.read(File.join(__dir__, "..", "test_data", "#{dataset}.json"))
    end

    def load_json_test_dataset(dataset)
      JSON.parse(load_raw_test_dataset(dataset))
    end
  end
end

unless ENV['RM_INFO']
  require "minitest/reporters"
  Minitest::Reporters.use!(
    [
      Minitest::Reporters::SpecReporter.new,
      Minitest::Reporters::JUnitReporter.new,
    ]
  )
end
