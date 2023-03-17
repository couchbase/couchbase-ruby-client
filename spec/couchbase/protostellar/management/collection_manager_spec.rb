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

RSpec.describe Couchbase::Protostellar::Management::CollectionManager do
  before(:all) do
    cluster = connect
    @collection_mgr = test_bucket(cluster).collections
  end

  def get_collection_spec(scope_name:, collection_name:)
    Couchbase::Management::CollectionSpec.new do |spec|
      spec.name = collection_name
      spec.scope_name = scope_name
    end
  end

  describe "#get_all_scopes" do
    subject(:scopes) { @collection_mgr.get_all_scopes }

    it "returns an array" do
      expect(scopes.class).to be Array
    end
  end

  describe "#create_scope" do
    context "when the scope does not currently exist" do
      it "creates the scope" do
        scope_name = unique_id(:scope)
        @collection_mgr.create_scope(scope_name)
        expect(@collection_mgr.get_all_scopes.any? { |scope_spec| scope_spec.name == scope_name }).to be true
      end
    end

    context "when the scope already exists" do
      it "raises a ScopeExists error" do
        scope_name = unique_id(:scope)
        @collection_mgr.create_scope(scope_name)
        expect { @collection_mgr.create_scope(scope_name) }.to raise_error(Couchbase::Error::ScopeExists)
      end
    end
  end

  describe "#drop_scope" do
    context "when the scope exists" do
      it "drops the scope" do
        scope_name = unique_id(:scope)
        @collection_mgr.create_scope(scope_name)
        @collection_mgr.drop_scope(scope_name)
        expect(@collection_mgr.get_all_scopes.any? { |scope_spec| scope_spec.name == scope_name }).to be false
      end
    end

    context "when the scope does not exist" do
      it "raises a ScopeNotFound error" do
        scope_name = unique_id(:nonexistent_scope)
        expect { @collection_mgr.drop_scope(scope_name) }.to raise_error(Couchbase::Error::ScopeNotFound)
      end
    end
  end

  describe "#create_collection" do
    before(:all) do
      @scope_name = unique_id(:scope)
      @collection_mgr.create_scope(@scope_name)
    end

    context "when the collection does not currently exist" do
      it "creates the collection" do
        coll_name = unique_id(:coll)
        @collection_mgr.create_collection(get_collection_spec(scope_name: @scope_name, collection_name: coll_name))
        scope_spec = @collection_mgr.get_all_scopes.find { |s| s.name == @scope_name }
        expect(scope_spec.collections.any? { |c| c.name == coll_name} ).to be true
      end
    end

    context "when the collection already exists" do
      it "raises a CollectionExists error" do
        coll_name = unique_id(:coll)
        coll_spec = get_collection_spec(scope_name: @scope_name, collection_name: coll_name)
        @collection_mgr.create_collection(coll_spec)
        expect { @collection_mgr.create_collection(coll_spec) }.to raise_error(Couchbase::Error::CollectionExists)
      end
    end
  end

  describe "#drop_collection" do
    before(:all) do
      @scope_name = unique_id(:scope)
      @collection_mgr.create_scope(@scope_name)
    end

    context "when the collection exists" do
      before do
        @coll_spec = get_collection_spec(scope_name: @scope_name, collection_name: unique_id(:coll))
        @collection_mgr.create_collection(@coll_spec)
      end

      it "drops the collection" do
        @collection_mgr.drop_collection(@coll_spec)
        scope_spec = @collection_mgr.get_all_scopes.find { |s| s.name == @scope_name }
        expect(scope_spec.collections.any? { |c| c.name == coll_name} ).to be false
      end
    end

    context "when the collection does not exist" do
      it "raises a CollectionNotFound error" do
        coll_spec = get_collection_spec(scope_name: @scope_name, collection_name: unique_id(:coll))
        expect { @collection_mgr.drop_collection(coll_spec) }.to raise_error(Couchbase::Error::CollectionNotFound)
      end
    end
  end
end
