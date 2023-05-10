RSpec.shared_examples "collection sub-document operations" do
  describe "#lookup_in" do
    let(:doc_id) { upsert_sample_document }

    context "when doing a get with the empty path" do
      let!(:lookup_in_result) do
        collection.lookup_in(doc_id, [Couchbase::LookupInSpec.get("")])
      end

      it "retrieves the entire document" do
        expect(lookup_in_result.content(0)).to eq sample_content
      end
    end

    context "when retrieving a sub-document value" do
      subject(:lookup_in_result) do
        collection.lookup_in(doc_id, [Couchbase::LookupInSpec.get("value")])
      end

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
