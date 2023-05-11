# frozen_string_literal: true

#  Copyright 2023. Couchbase, Inc.
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

require "couchbase/protostellar"

module Helpers
  class TestEnvironment
    DEFAULT_CLASSIC_CONN_STRING = "couchbase://localhost"
    DEFAULT_CERT_PATH = File.absolute_path('cert.pem')
    DEFAULT_PROTOSTELLAR_CONN_STRING = "protostellar://localhost?trust_certificate=#{DEFAULT_CERT_PATH}"
    DEFAULT_USERNAME = "Administrator"
    DEFAULT_PASSWORD = "password"
    DEFAULT_BUCKET = "default"

    def classic_connection_string
      @classic_connection_string ||= ENV.fetch("TEST_CONNECTION_STRING", DEFAULT_CLASSIC_CONN_STRING)
    end

    def protostellar_connection_string
      @protostellar_connection_string ||= ENV.fetch("TEST_PROTOSTELLAR_CONNECTION_STRING", DEFAULT_PROTOSTELLAR_CONN_STRING)
    end

    def username
      @username ||= ENV.fetch("TEST_USERNAME", DEFAULT_USERNAME)
    end

    def password
      @password ||= ENV.fetch("TEST_PASSWORD", DEFAULT_PASSWORD)
    end

    def bucket
      @bucket ||= ENV.fetch("TEST_BUCKET", DEFAULT_BUCKET)
    end
  end

  def connect_with_classic
    connect(env.classic_connection_string)
  end

  def connect_with_protostellar
    connect(env.protostellar_connection_string)
  end

  def connect(conn_str = nil)
    conn_str ||= env.classic_connection_string
    opts = Couchbase::Options::Cluster.new
    opts.authenticate(env.username, env.password)
    Couchbase::Cluster.connect(conn_str, opts)
  end

  def env
    @env ||= TestEnvironment.new
  end

  def unique_id(name)
    # TODO: Remove all references of this
    uniq_id(name)
  end

  def uniq_id(name)
    parent = caller_locations&.first
    prefix = "#{File.basename(parent&.path, '.rb')}_#{parent&.lineno}"
    "#{prefix}_#{name}_#{Time.now.to_f.to_s.reverse}".tr('.', '_')
  end

  def default_collection(cluster, bucket_name: nil)
    bucket_name ||= env.bucket
    cluster.bucket(bucket_name).default_collection
  end

  def default_scope(cluster, bucket_name: nil)
    bucket_name ||= env.bucket
    cluster.bucket(bucket_name).default_scope
  end

  def test_bucket(cluster, bucket_name: nil)
    bucket_name ||= env.bucket
    cluster.bucket(bucket_name)
  end

  def use_caves?
    # TODO: Temporary
    false
  end

  def load_raw_test_dataset(dataset)
    File.read(File.join(__dir__, "..", "test_data", "#{dataset}.json"))
  end

  def load_json_test_dataset(dataset)
    JSON.parse(load_raw_test_dataset(dataset))
  end
end
