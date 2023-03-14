# frozen_string_literal: true

#  Copyright 2022-Present Couchbase, Inc.
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
require "couchbase/protostellar"

RSpec.describe Couchbase::Protostellar::Collection do
  let(:cluster) do
    connect
  end

  let(:collection) do
    default_collection(cluster)
  end

  let(:sample_content) do
    {"value" => 42}
  end

  let(:updated_content) do
    {"updated value" => 100}
  end

  def upsert_sample_document(name: :foo, content: sample_content, doc_id: nil)
    doc_id = unique_id(name) if doc_id.nil?
    collection.upsert(doc_id, content)
    doc_id
  end

  describe "#get" do
    context "when the document exists" do
      before do
        @doc_id = upsert_sample_document(name: :foo)
        @get_result = collection.get(@doc_id)
      end

      after do
        collection.remove(@doc_id)
      end

      it "the result has non-zero CAS" do
        expect(@get_result.cas != 0).to be true
      end

      it "the result has success set to true" do
        expect(@get_result.success?).to be true
      end

      it "the result has the correct content" do
        expect(@get_result.content).to eq(sample_content)
      end

      it "the content of the document remains unchanged" do
        expect(collection.get(@doc_id).content).to eq(@get_result.content)
      end
    end

    context "when the document does not exist" do
      it "raises DocumentNotFound error" do
        expect { collection.get(unique_id(name: :foo)) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end

  describe "#upsert" do
    context "when the document already exists" do
      before do
        @doc_id = upsert_sample_document(name: :foo)
        @mutation_result = collection.upsert(@doc_id, updated_content)
      end

      after do
        collection.remove(@doc_id)
      end

      it "the result has non-zero CAS" do
        expect(@mutation_result.cas != 0).to be true
      end

      it "the result has success set to true" do
        expect(@mutation_result.success?).to be true
      end

      it "the content of the document has been updated" do
        expect(collection.get(@doc_id).content).to eq(updated_content)
      end
    end

    context "when the document does not currently exist" do
      before do
        @doc_id = unique_id(:foo)
        @mutation_result = collection.upsert(@doc_id, sample_content)
      end

      after do
        collection.remove(@doc_id)
      end

      it "the result has non-zero CAS" do
        expect(@mutation_result.cas != 0).to be true
      end

      it "the result has success set to true" do
        expect(@mutation_result.success?).to be true
      end

      it "the document has been inserted and has the correct content" do
        expect(collection.get(@doc_id).content).to eq(sample_content)
      end
    end
  end

  describe "#insert" do
    context "when the document does not currently exist" do
      before do
        @doc_id = unique_id(:foo)
        @mutation_result = collection.insert(@doc_id, sample_content)
      end

      after do
        collection.remove(@doc_id)
      end

      it "the result has non-zero CAS" do
        expect(@mutation_result.cas != 0).to be true
      end

      it "the result has success set to true" do
        expect(@mutation_result.success?).to be true
      end

      it "the document has been inserted and has the correct content" do
        expect(collection.get(@doc_id).content).to eq(sample_content)
      end
    end

    context "when the document already exists" do
      before do
        @doc_id = upsert_sample_document
      end

      after do
        collection.remove(@doc_id)
      end

      it "raises DocumentExists error" do
        expect { collection.insert(@doc_id, sample_content) }.to raise_error(Couchbase::Error::DocumentExists)
      end
    end
  end

  describe "#replace" do
    context "when the document exists" do
      before do
        @doc_id = upsert_sample_document
        @mutation_result = collection.replace(@doc_id, updated_content)
      end

      after do
        collection.remove(@doc_id)
      end

      it "the result has non-zero CAS" do
        expect(@mutation_result.cas != 0).to be true
      end

      it "the result has success set to true" do
        expect(@mutation_result.success?).to be true
      end

      it "the document has been replaced and has the correct content" do
        expect(collection.get(@doc_id).content).to eq(updated_content)
      end
    end

    context "when the document does not exist" do
      before do
        @doc_id = unique_id(:foo)
      end

      it "raises DocumentNotFound error" do
        expect { collection.replace(@doc_id, updated_content) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end

  describe "#remove" do
    context "when the document exists" do
      before do
        @doc_id = upsert_sample_document
        @mutation_result = collection.remove(@doc_id)
      end

      it "the result has non-zero CAS" do
        expect(@mutation_result.cas != 0).to be true
      end

      it "the result has success set to true" do
        expect(@mutation_result.success?).to be true
      end

      it "the document no longer exists" do
        expect { collection.get(@doc_id) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document does not exist" do
      before do
        @doc_id = unique_id(:foo)
      end

      it "raises DocumentNotFound error" do
        expect { collection.remove(@doc_id) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end

  describe "#exists" do
    context "when the document exists" do
      before do
        @doc_id = upsert_sample_document
        @exists_result = collection.exists(@doc_id)
      end

      after do
        collection.remove(@doc_id)
      end

      it "the result has non-zero CAS" do
        expect(@exists_result.cas != 0).to be true
      end

      it "the `exists` field in the result is set to true" do
        expect(@exists_result.exists?).to be true
      end
    end

    context "when the document does not exist" do
      before do
        @doc_id = unique_id(:foo)
        @exists_result = collection.exists(@doc_id)
      end

      it "the result has zero CAS" do
        expect(@exists_result.cas).to be 0
      end

      it "the `exists` field in the result is set to false" do
        expect(@exists_result.exists?).to be false
      end
    end
  end

  describe "#lookup_in" do
    context "when retrieving a sub-document value" do
      subject(:lookup_in_result) do
        collection.lookup_in(doc_id, [Couchbase::LookupInSpec.get("value")])
      end

      let(:doc_id) { upsert_sample_document }

      it { expect(lookup_in_result.exists?(0)).to be true }
      it { expect(lookup_in_result.content(0)).to be 42 }
      it { expect(lookup_in_result.cas).not_to be 0 }
    end

    context "when checking if a path exists" do
      subject(:lookup_in_result) do
        collection.lookup_in(doc_id, [Couchbase::LookupInSpec.exists("value")])
      end

      let(:doc_id) { upsert_sample_document }

      it { expect(lookup_in_result.exists?(0)).to be true }
      it { expect(lookup_in_result.content(0)).to be_nil }
      it { expect(lookup_in_result.cas).not_to be 0 }
    end

    context "when checking the existence of a non-existent path" do
      subject(:lookup_in_result) do
        collection.lookup_in(doc_id, [Couchbase::LookupInSpec.exists("number")])
      end

      let(:doc_id) { upsert_sample_document }

      it { expect(lookup_in_result.exists?(0)).to be false }
      it { expect { lookup_in_result.content(0) }.to raise_error(Couchbase::Error::PathNotFound) }
      it { expect(lookup_in_result.cas).not_to be 0 }
    end
  end

  describe "#mutate_in" do
    context "when upserting a sub-document" do
      subject!(:mutate_in_result) do
        collection.mutate_in(doc_id, [Couchbase::MutateInSpec.upsert("value", "30")])
      end

      let(:doc_id) { upsert_sample_document }

      it { expect(mutate_in_result.content(0)).to be "30" }
    end
  end
end
