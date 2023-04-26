# frozen_string_literal: true

#  Copyright 2022-Present. Couchbase, Inc.
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

  DEFAULT_HOST = "localhost"
  DEFAULT_USERNAME = "Administrator"
  DEFAULT_PASSWORD = "password"
  DEFAULT_BUCKET = "default"
  DEFAULT_CERTIFICATE_PATH = File.join(__dir__, "../cert.pem")

  def connect(scheme = "protostellar", host: DEFAULT_HOST, username: DEFAULT_USERNAME, password: DEFAULT_PASSWORD)
    conn_str = "#{scheme}://#{host}"
    opts = Couchbase::Options::Cluster.new
    opts.authenticate(username, password)
    Couchbase::Cluster.connect(conn_str, opts)
  end

  def unique_id(name)
    parent = caller_locations&.first
    prefix = "#{File.basename(parent&.path, '.rb')}_#{parent&.lineno}"
    "#{prefix}_#{name}_#{Time.now.to_f.to_s.reverse}".tr('.', '_')
  end

  def uniq_id(name)
    parent = caller_locations&.first
    prefix = "#{File.basename(parent&.path, '.rb')}_#{parent&.lineno}"
    "#{prefix}_#{name}_#{Time.now.to_f.to_s.reverse}".tr('.', '_')
  end

  def default_collection(cluster, bucket_name: DEFAULT_BUCKET)
    cluster.bucket(bucket_name).default_collection
  end

  def default_scope(cluster, bucket_name: DEFAULT_BUCKET)
    cluster.bucket(bucket_name).default_scope
  end

  def test_bucket(cluster, bucket_name: DEFAULT_BUCKET)
    cluster.bucket(bucket_name)
  end

  def sample_content
    {:content => "sample"}
  end

  def use_caves?
    false
  end

  def load_raw_test_dataset(dataset)
    File.read(File.join(__dir__, "..", "test_data", "#{dataset}.json"))
  end

  def load_json_test_dataset(dataset)
    JSON.parse(load_raw_test_dataset(dataset))
  end

  def test_certificate(filename: DEFAULT_CERTIFICATE_PATH)
    File.read(filename)
  end
end
