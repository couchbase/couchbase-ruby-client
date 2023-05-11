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

RSpec.shared_examples "collection sub-document operations" do
  let(:sample_content) do
    {"value" => 42}
  end

  def upsert_sample_document(name: :foo, content: sample_content, doc_id: nil, options: nil)
    doc_id = uniq_id(name) if doc_id.nil?

    if options.nil?
      collection.upsert(doc_id, content)
    else
      collection.upsert(doc_id, content, options)
    end

    doc_id
  end

  describe "#lookup_in" do
    context "when loading primitive fields" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => "bar", "num" => 1234}) }

      let!(:result) do
        collection.lookup_in(
          doc_id,
          [
            Couchbase::LookupInSpec.get("foo"),
            Couchbase::LookupInSpec.get("num"),
          ]
        )
      end

      it "the result includes the content for both paths" do
        expect(result.content(0)).to eq("bar")
        expect(result.content(1)).to eq(1234)
      end

      it "the result specifies that values for both paths exist" do
        expect(result.exists?(0)).to be true
        expect(result.exists?(1)).to be true
        expect(result.exists?(2)).to be false
      end

      it "the CAS of the result is non-zero" do
        expect(result.cas).not_to be_zero
      end
    end

    context "when loading an object and an array" do
      let(:doc_id) { upsert_sample_document(content: {"obj" => {}, "arr" => []}) }

      let!(:result) do
        collection.lookup_in(
          doc_id,
          [
            Couchbase::LookupInSpec.get("obj"),
            Couchbase::LookupInSpec.get("arr"),
          ]
        )
      end

      it "the result includes the content for both paths" do
        expect(result.content(0)).to be_empty
        expect(result.content(1)).to be_empty
      end

      it "the result specifies that values for both paths exist" do
        expect(result.exists?(0)).to be true
        expect(result.exists?(1)).to be true
      end

      it "the CAS of the result is non-zero" do
        expect(result.cas).not_to be_zero
      end
    end

    context "when there are no commands" do
      let(:doc_id) { upsert_sample_document }

      it "raises an ArgumentError" do
        expect { collection.lookup_in(doc_id, []) }.to raise_error(ArgumentError)
      end
    end

    context "when loading one path that does not exist" do
      let(:doc_id) { upsert_sample_document }

      let!(:result) do
        collection.lookup_in(
          doc_id,
          [
            Couchbase::LookupInSpec.get("does_not_exist"),
          ]
        )
      end

      it "attempting to get the content raises a PathNotFound error" do
        expect { result.content(0) }.to raise_error(Couchbase::Error::PathNotFound)
      end

      it "the result specifies that the value for the path does not exist" do
        expect(result.exists?(0)).to be false
      end
    end

    context "when loading multiple paths of which the first does not exist" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => "bar"}) }

      let!(:result) do
        collection.lookup_in(
          doc_id,
          [
            Couchbase::LookupInSpec.get("does_not_exist"),
            Couchbase::LookupInSpec.get("foo"),
          ]
        )
      end

      it "attempting to get the content for the first path raises a PathNotFound error" do
        expect { result.content(0) }.to raise_error(Couchbase::Error::PathNotFound)
      end

      it "the result includes the correct content for the second path" do
        expect(result.content(1)).to eq("bar")
      end

      it "the result specifies that the value for the first path does not exist" do
        expect(result.exists?(0)).to be false
      end

      it "the result specifies that the value for the second path exists" do
        expect(result.exists?(1)).to be true
      end
    end

    context "when checking for the existence of a path that does not exist" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => "bar"}) }

      let!(:result) do
        collection.lookup_in(
          doc_id,
          [
            Couchbase::LookupInSpec.exists("does_not_exist"),
          ]
        )
      end

      it "the result specifies that the value does not exist" do
        expect(result.exists?(0)).to be false
      end

      it "attempting to get the content raises a PathNotFound error" do
        expect { result.content(0) }.to raise_error(Couchbase::Error::PathNotFound)
      end
    end

    context "when checking the existence of multiple paths of which the first does not exist" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => "bar"}) }

      let!(:result) do
        collection.lookup_in(
          doc_id,
          [
            Couchbase::LookupInSpec.exists("does_not_exist"),
            Couchbase::LookupInSpec.exists("foo"),
          ]
        )
      end

      it "attempting to get the content for the first path raises a PathNotFound error" do
        expect { result.content(0) }.to raise_error(Couchbase::Error::PathNotFound)
      end

      it "the result specifies that the value for the first path does not exist" do
        expect(result.exists?(0)).to be false
      end

      it "the result specifies that the value for the second path exists" do
        expect(result.exists?(1)).to be true
      end
    end

    context "when getting the count for an array" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => %w[hello world]}) }

      let!(:result) do
        collection.lookup_in(
          doc_id,
          [
            Couchbase::LookupInSpec.count("foo"),
          ]
        )
      end

      it "the result specifies that the value for the path exists" do
        expect(result.exists?(0)).to be true
      end

      it "the content for the path is set to the number of elements in the array" do
        expect(result.content(0)).to be 2
      end
    end

    context "when doing a get with the empty path" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => "bar"}) }

      let!(:result) do
        collection.lookup_in(
          doc_id,
          [
            Couchbase::LookupInSpec.get(""),
          ]
        )
      end

      it "returns the content of the entire document" do
        expect(result.content(0)).to eq({"foo" => "bar"})
      end
    end
  end

  describe "#mutate_in" do
    context "when incrementing a counter" do
      let(:doc_id) { upsert_sample_document }

      let!(:mutate_in_result) do
        collection.mutate_in(
          doc_id,
          [
            Couchbase::MutateInSpec.increment("value", 4),
          ]
        )
      end

      it "the counter is incremented by the specified delta" do
        expect(collection.get(doc_id).content).to eq({"value" => 46})
      end
    end

    context "when there are no commands" do
      let(:doc_id) { upsert_sample_document }

      it "raises an ArgumentError" do
        expect { collection.mutate_in(doc_id, []) }.to raise_error(ArgumentError)
      end
    end

    context "when inserting a primitive field" do
      let(:doc_id) { upsert_sample_document(content: {}) }

      let!(:mutate_in_result) do
        collection.mutate_in(
          doc_id,
          [
            Couchbase::MutateInSpec.insert("foo", "bar"),
          ]
        )
      end

      it "the CAS of the result is not zero" do
        expect(mutate_in_result.cas).not_to be_zero
      end

      it "the fields have been added to the document" do
        expect(collection.get(doc_id).content).to eq({"foo" => "bar"})
      end
    end

    context "when upserting a full document" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => "bar"}) }

      let!(:mutate_in_result) do
        options = Couchbase::Collection::MutateInOptions.new
        options.store_semantics = :upsert
        collection.mutate_in(
          doc_id,
          [
            # full document mutation should also carry XATTR spec
            Couchbase::MutateInSpec.upsert("my_model.revision", 42).xattr.create_path,
            Couchbase::MutateInSpec.replace("", {"bar" => "foo"}),
          ],
          options
        )
      end

      it "the entire content of the document has been updated" do
        expect(collection.get(doc_id).content).to eq({"bar" => "foo"})
      end
    end

    context "when upserting a full document with an XAttr operation last" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => "bar"}) }

      let!(:mutate_in_result) do
        options = Couchbase::Collection::MutateInOptions.new
        options.store_semantics = :upsert
        collection.mutate_in(
          doc_id,
          [
            # full document mutation should also carry XATTR spec
            Couchbase::MutateInSpec.replace("", {"bar" => "foo"}),
            Couchbase::MutateInSpec.upsert("my_model.revision", 42).xattr.create_path,
          ],
          options
        )
      end

      it "the entire content of the document has been updated" do
        expect(collection.get(doc_id).content).to eq({"bar" => "foo"})
      end
    end

    context "when inserting a full document" do
      let(:doc_id) { uniq_id(:foo) }

      let!(:mutate_in_result) do
        options = Couchbase::Collection::MutateInOptions.new
        options.store_semantics = :insert
        collection.mutate_in(
          doc_id,
          [
            # full document mutation should also carry XATTR spec
            Couchbase::MutateInSpec.upsert("my_model.revision", 42).xattr.create_path,
            Couchbase::MutateInSpec.replace("", {"bar" => "foo"}),
          ],
          options
        )
      end

      it "the document has been created" do
        expect(collection.get(doc_id).content).to eq({"bar" => "foo"})
      end
    end

    context "when removing a full document" do
      let(:doc_id) { upsert_sample_document(content: {"foo" => "bar"}) }

      let!(:mutate_in_result) do
        collection.mutate_in(
          doc_id,
          [
            # full document mutation should also carry XATTR spec
            Couchbase::MutateInSpec.upsert("my_model.revision", 42).xattr.create_path,
            Couchbase::MutateInSpec.remove(""),
          ]
        )
      end

      it "the document has no longer be retrieved" do
        expect { collection.get(doc_id) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end

      it "the document is marked as deleted" do
        lookup_in_result = collection.lookup_in(
          doc_id, [Couchbase::LookupInSpec.get("$document").xattr],
          Couchbase::Options::LookupIn.new(access_deleted: true)
        )
        expect(lookup_in_result.content(0)["CAS"].to_i(16)).to be(mutate_in_result.cas)
      end
    end

    context "withg multiple operations including a counter increment" do
      let(:doc_id) do
        upsert_sample_document(content: {
          "mutated" => 0,
          "body" => "",
          "first_name" => "James",
          "age" => 0,
        })
      end

      let!(:mutate_in_result) do
        collection.mutate_in(
          doc_id,
          [
            Couchbase::MutateInSpec.upsert("addr", {
              "state" => "NV",
              "pincode" => 7,
              "city" => "Chicago",
            }),
            Couchbase::MutateInSpec.increment("mutated", 1),
            Couchbase::MutateInSpec.upsert("name", {
              "last" => "",
              "first" => "James",
            }),
          ]
        )
      end

      let(:get_result) { collection.get(doc_id) }

      let(:expected_content) do
        {
          "mutated" => 1,
          "body" => "",
          "first_name" => "James",
          "age" => 0,
          "addr" => {
            "state" => "NV",
            "pincode" => 7,
            "city" => "Chicago",
          },
          "name" => {
            "last" => "",
            "first" => "James",
          },
        }
      end

      it "the counter has been incremented by an amount equal to delta" do
        expect(get_result.content["mutated"]).to be 1
      end

      it "the document has the correct content after the mutations" do
        expect(get_result.content).to eq(expected_content)
      end
    end

    context "with a single counter increment" do
      let(:doc_id) do
        upsert_sample_document(content: {
          "mutated" => 0,
          "body" => "",
          "first_name" => "James",
          "age" => 0,
        })
      end

      let!(:result) do
        collection.mutate_in(
          doc_id,
          [
            Couchbase::MutateInSpec.increment("mutated", 3),
          ]
        )
      end

      it "the counter has been incremented by an amount equal to delta" do
        expect(collection.get(doc_id).content["mutated"]).to be 3
      end
    end

    context "with a single counter decrement" do
      let(:doc_id) { upsert_sample_document(content: {"value" => 20}) }

      let!(:result) do
        collection.mutate_in(
          doc_id,
          [
            Couchbase::MutateInSpec.decrement("value", 3),
          ]
        )
      end

      it "the counter has been decremented by an amount equal to delta" do
        expect(collection.get(doc_id).content["value"]).to be 17
      end
    end

    context "with operations to both increment and decrement the same counter" do
      let(:doc_id) { upsert_sample_document(content: {"value" => 20}) }

      let!(:result) do
        collection.mutate_in(
          doc_id,
          [
            Couchbase::MutateInSpec.increment("value", 5),
            Couchbase::MutateInSpec.decrement("value", 11),
          ]
        )
      end

      it "the value of the counter is the expected result after both counter operations are executed" do
        expect(collection.get(doc_id).content["value"]).to be 14
      end
    end
  end
end
