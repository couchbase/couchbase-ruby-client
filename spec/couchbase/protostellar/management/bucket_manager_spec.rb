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

require "rspec"

RSpec.describe Couchbase::Protostellar::Management::BucketManager do
  let(:cluster) { connect_with_protostellar }
  let(:bucket_name) { unique_id(:bucket) }
  let(:mgr) { cluster.buckets }

  after do
    mgr.drop_bucket(bucket_name)
  rescue Couchbase::Error::BucketNotFound
    nil
  end

  describe "#create_bucket" do
    context "with Couchbase bucket type" do
      let(:settings) do
        Couchbase::Management::BucketSettings.new do |s|
          s.name = bucket_name
          s.bucket_type = :couchbase
        end
      end

      let(:res) do
        mgr.create_bucket(settings)
        mgr.get_bucket(bucket_name)
      end

      it "creates a bucket of type Couchbase" do
        expect(res.name).to be eq(bucket_name)
        expect(res.bucket_type).to be :couchbase
      end
    end

    context "with Memcached bucket type" do
      let(:settings) do
        Couchbase::Management::BucketSettings.new do |s|
          s.name = bucket_name
          s.bucket_type = :memcached
        end
      end

      let(:res) do
        mgr.create_bucket(settings)
        mgr.get_bucket(bucket_name)
      end

      it "creates a bucket of type Memcached" do
        expect(res.name).to be eq(bucket_name)
        expect(res.bucket_type).to be :memcached
      end
    end

    context "with Ephemeral bucket type" do
      let(:settings) do
        Couchbase::Management::BucketSettings.new do |s|
          s.name = bucket_name
          s.bucket_type = :ephemeral
        end
      end

      let(:res) do
        mgr.create_bucket(settings)
        mgr.get_bucket(bucket_name)
      end

      it "creates a bucket of type Couchbase" do
        expect(res.name).to be eq(bucket_name)
        expect(res.bucket_type).to be :couchbase
      end
    end
  end
end
