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

RSpec.describe Couchbase::Protostellar::BinaryCollection do
  let(:cluster) do
    connect
  end

  let(:collection) do
    default_collection(cluster)
  end

  let(:binary_collection) do
    collection.binary
  end

  describe "#increment" do
    context "when the document does not exist and initial is not defined" do
      it "raises a DocumentNotFoundError" do
        expect { binary_collection.increment(unique_id(:counter)) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document does not exist and initial is defined" do
      it "sets the value of the counter to the initial value" do
        opts = Couchbase::Options::Increment(initial: 12)
        res = binary_collection.increment(unique_id(:counter), opts)
        expect(res.content).to be 12
      end
    end

    context "when the document exists" do
      before do
        @doc_id = unique_id(:counter)
        collection.upsert(@doc_id, 20)
      end

      it "increments the counter by one" do
        res = binary_collection.increment(@doc_id)
        expect(res.content).to be 21
      end
    end

    context "when the document exists and delta is set" do
      before do
        @doc_id = unique_id(:counter)
        collection.upsert(@doc_id, 20)
      end

      it "increments the counter by the value of delta" do
        opts = Couchbase::Options::Increment(delta: 20)
        res = binary_collection.increment(@doc_id, opts)
        expect(res.content).to be 40
      end
    end
  end

  describe "#decrement" do
    context "when decrementing a document that does not exist without defining an initial value" do
      it "raises a DocumentNotFoundError" do
        expect { binary_collection.decrement(unique_id(:counter)) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document does not exist and initial is defined" do
      it "sets the value of the counter to the initial value" do
        opts = Couchbase::Options::Decrement(initial: 12)
        res = binary_collection.decrement(unique_id(:counter), opts)
        expect(res.content).to be 12
      end
    end

    context "when the document exists" do
      before do
        @doc_id = unique_id(:counter)
        collection.upsert(@doc_id, 20)
      end

      it "decrements the counter by one" do
        res = binary_collection.decrement(@doc_id)
        expect(res.content).to be 19
      end
    end

    context "when the document exists and delta is set" do
      before do
        @doc_id = unique_id(:counter)
        collection.upsert(@doc_id, 20)
      end

      it "decrements the counter by the value of delta" do
        opts = Couchbase::Options::Decrement(delta: 11)
        res = binary_collection.decrement(@doc_id, opts)
        expect(res.content).to be 9
      end
    end
  end

  describe "#append" do
    context "when the document does not exist" do
      it "raises a DocumentNotFound error" do
        expect { binary_collection.append(unique_id(:append), "something") }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document exists" do
      before do
        @doc_id = unique_id(:append)
        collection.upsert(@doc_id, "|init|", Couchbase::Options::Upsert(transcoder: nil))
        @mutation_result = binary_collection.append(@doc_id, "something")
      end

      it "the result has non-zero CAS" do
        expect(@mutation_result.cas).not_to be 0
      end

      it "the document has the correct content" do
        expect(collection.get(@doc_id, Couchbase::Options::Get(transcoder: nil)).content).to eq("|init|something")
      end
    end
  end

  describe "#prepend" do
    context "when the document does not exist" do
      it "raises a DocumentNotFound error" do
        expect { binary_collection.prepend(unique_id(:prepend), "something") }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document exists" do
      before do
        @doc_id = unique_id(:prepend)
        collection.upsert(@doc_id, "|init|", Couchbase::Options::Upsert(transcoder: nil))
        @mutation_result = binary_collection.prepend(@doc_id, "something")
      end

      it "the result has non-zero CAS" do
        expect(@mutation_result.cas).not_to be 0
      end

      it "the document has the correct content" do
        expect(collection.get(@doc_id, Couchbase::Options::Get(transcoder: nil)).content).to eq("something|init|")
      end
    end
  end
end
