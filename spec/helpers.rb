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
  include Couchbase::Protostellar

  DEFAULT_HOST = "localhost:18098"
  DEFAULT_USERNAME = "Administrator"
  DEFAULT_PASSWORD = "password"
  DEFAULT_BUCKET = "default"

  def connect(host: DEFAULT_HOST, options: ConnectOptions.new(username: DEFAULT_USERNAME, password: DEFAULT_PASSWORD))
    Cluster.new(host, options)
  end

  def unique_id(name)
    parent = caller_locations&.first
    prefix = "#{File.basename(parent&.path, '.rb')}_#{parent&.lineno}"
    "#{prefix}_#{name}_#{Time.now.to_f.to_s.reverse}"
  end

  def default_collection(cluster, bucket_name: DEFAULT_BUCKET)
    cluster.bucket(bucket_name).default_collection
  end

  def test_bucket(cluster, bucket_name: DEFAULT_BUCKET)
    cluster.bucket(bucket_name)
  end

  def sample_content
    {:content => "sample"}
  end
end
