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

require_relative 'utils/consistency_helper'

require "couchbase/management"

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

  def supports_mad_hatter_subdoc_macros?
    mad_hatter? || cheshire_cat?
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

  def supports_history_retention?
    @version >= Gem::Version.create("7.2.0")
  end

  def supports_update_collection?
    @version >= Gem::Version.create("7.2.0")
  end

  def supports_update_collection_max_expiry?
    trinity?
  end

  def supports_collection_max_expiry_set_to_no_expiry?
    trinity?
  end

  def supports_scoped_search_indexes?
    trinity?
  end

  def supports_vector_search?
    trinity?
  end

  def supports_multiple_xattr_keys_mutation?
    trinity?
  end

  def supports_server_group_replica_reads?
    @version >= Gem::Version.create("7.6.2")
  end
end

require "couchbase"
require "couchbase/protostellar"
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

    def management_endpoint
      @management_endpoint = ENV.fetch("TEST_MANAGEMENT_ENDPOINT") do
        if connection_string
          parsed = Couchbase::Backend.parse_connection_string(connection_string)
          first_node_address = parsed[:nodes].first[:address]
          scheme = parsed[:tls] ? "https" : "http"
          port = parsed[:tls] ? 18091 : 8091
          "#{scheme}://#{first_node_address}:#{port}"
        end
      end
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

    def protostellar?
      Couchbase::Protostellar::SCHEMES.any? { |s| !@connection_string[s].nil? }
    end

    def consistency
      @consistency ||=
        if ENV.fetch("TEST_CONNECTION_STRING", nil)
          TestUtilities::ConsistencyHelper.new(management_endpoint, username, password)
        else
          TestUtilities::MockConsistencyHelper.new
        end
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
      env
      defined? @caves
    end

    def use_server?
      !use_caves?
    end

    def connect(options = Options::Cluster.new)
      options.authenticate(env.username, env.password)
      @cluster = Cluster.connect(env.connection_string, options)
    end

    def ensure_primary_index!
      @cluster ||= connect
      index_manager = @cluster.query_indexes
      index_manager.create_primary_index(env.bucket, Management::Options::Query::CreatePrimaryIndex.new(ignore_if_exists: true))
      sleep 0.1 while index_manager.get_all_indexes(env.bucket).none? { |idx| idx.name == "#primary" && idx.state == :online }
    end

    def disconnect
      return unless defined?(@cluster) && @cluster

      @cluster.disconnect
      @cluster = nil
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

    def retry_for_duration(expected_errors:, duration: 10, backoff: 1)
      deadline = Time.now + duration
      begin
        yield
      rescue StandardError => e
        raise e unless expected_errors.include?(e.class) && Time.now < deadline

        sleep(backoff)
        retry
      end
    end

    def retry_until_error(error:, duration: 10, backoff: 1)
      deadline = Time.now + duration
      begin
        yield
      rescue StandardError => e
        return if e.is_a?(error)

        raise "Did not get error #{error} within #{duration} seconds" unless Time.now < deadline

        sleep(backoff)
        retry
      end
    end
  end
end

unless ENV['RM_INFO']
  require "minitest/reporters"
  Minitest::Reporters.use!(
    [
      Minitest::Reporters::SpecReporter.new(print_failure_summary: true),
      Minitest::Reporters::JUnitReporter.new(Minitest::Reporters::JUnitReporter::DEFAULT_REPORTS_DIR, true, include_timestamp: true),
    ],
  )
end
