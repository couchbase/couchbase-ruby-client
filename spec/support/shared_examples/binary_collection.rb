require "rspec"

require "couchbase/raw_binary_transcoder"

RSpec.shared_examples "a binary collection" do
  describe "#increment" do
    context "when the document does not exist" do
      context "when initial is not defined" do
        it "raises a DocumentNotFoundError" do
          expect { binary_collection.increment(uniq_id(:counter)) }.to raise_error(Couchbase::Error::DocumentNotFound)
        end
      end

      context "when initial is defined" do
        let(:doc_id) { uniq_id(:counter) }

        let!(:counter_result) do
          options = Couchbase::Options::Increment.new(initial: 12)
          binary_collection.increment(doc_id, options)
        end

        it "creates the document" do
          expect { collection.get(doc_id) }.not_to raise_error
        end

        it "sets the value of the counter to the initial value" do
          expect(counter_result.content).to be 12
        end
      end
    end

    context "when the document exists" do
      let(:doc_id) do
        doc_id = uniq_id(:counter)
        collection.upsert(doc_id, 20)
        doc_id
      end

      context "when delta is not set" do
        it "increments the counter by one" do
          res = binary_collection.increment(doc_id)
          expect(res.content).to be 21
        end
      end

      context "when a custom delta is set" do
        it "increments the counter by the value of delta" do
          opts = Couchbase::Options::Increment(delta: 20)
          res = binary_collection.increment(doc_id, opts)
          expect(res.content).to be 40
        end
      end
    end
  end

  describe "#decrement" do
    context "when the document does not exist" do
      context "when initial is not defined" do
        it "raises a DocumentNotFoundError" do
          expect { binary_collection.decrement(uniq_id(:counter)) }.to raise_error(Couchbase::Error::DocumentNotFound)
        end
      end

      context "when initial is defined" do
        let(:doc_id) { uniq_id(:counter) }

        let!(:counter_result) do
          options = Couchbase::Options::Decrement.new(initial: 12)
          binary_collection.decrement(doc_id, options)
        end

        it "creates the document" do
          expect { collection.get(doc_id) }.not_to raise_error
        end

        it "sets the value of the counter to the initial value" do
          expect(counter_result.content).to be 12
        end
      end
    end

    context "when the document exists" do
      let(:doc_id) do
        doc_id = uniq_id(:counter)
        collection.upsert(doc_id, 20)
        doc_id
      end

      context "when delta is not set" do
        it "decrements the counter by one" do
          res = binary_collection.decrement(doc_id)
          expect(res.content).to be 19
        end
      end

      context "when a custom delta is set" do
        it "decrements the counter by the value of delta" do
          opts = Couchbase::Options::Decrement(delta: 20)
          res = binary_collection.decrement(doc_id, opts)
          expect(res.content).to be_zero
        end
      end
    end
  end

  describe "#append" do
    context "when the document does not exist" do
      it "raises a DocumentNotFound error" do
        expect { binary_collection.append(unique_id(:append), "bar") }
          .to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document exists" do
      let(:doc_id) do
        doc_id = uniq_id(:append)
        options = Couchbase::Options::Upsert.new(transcoder: Couchbase::RawBinaryTranscoder.new)
        collection.upsert(doc_id, "foo", options)
        doc_id
      end

      let!(:result) { binary_collection.append(doc_id, "bar") }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the document has the correct content" do
        options = Couchbase::Options::Get(transcoder: Couchbase::RawBinaryTranscoder.new)
        expect(collection.get(doc_id, options).content).to eq("foobar")
      end
    end

    context "when durability is set" do
      let(:doc_id) do
        doc_id = uniq_id(:append)
        options = Couchbase::Options::Upsert.new(transcoder: Couchbase::RawBinaryTranscoder.new)
        collection.upsert(doc_id, "foo", options)
        doc_id
      end

      let!(:result) do
        options = Couchbase::Options::Append.new(durability_level: :majority)
        binary_collection.append(doc_id, "bar", options)
      end

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the document has the correct content" do
        options = Couchbase::Options::Get.new(transcoder: Couchbase::RawBinaryTranscoder.new)
        expect(collection.get(doc_id, options).content).to eq("foobar")
      end
    end
  end

  describe "#prepend" do
    context "when the document does not exist" do
      it "raises a DocumentNotFound error" do
        expect { binary_collection.prepend(unique_id(:prepend), "bar") }
          .to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document exists" do
      let(:doc_id) do
        doc_id = uniq_id(:prepend)
        options = Couchbase::Options::Upsert.new(transcoder: Couchbase::RawBinaryTranscoder.new)
        collection.upsert(doc_id, "foo", options)
        doc_id
      end

      let!(:result) { binary_collection.prepend(doc_id, "bar") }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the document has the correct content" do
        options = Couchbase::Options::Get.new(transcoder: Couchbase::RawBinaryTranscoder.new)
        expect(collection.get(doc_id, options).content).to eq("barfoo")
      end
    end

    context "when durability is set" do
      let(:doc_id) do
        doc_id = uniq_id(:prepend)
        options = Couchbase::Options::Upsert.new(transcoder: Couchbase::RawBinaryTranscoder.new)
        collection.upsert(doc_id, "foo", options)
        doc_id
      end

      let!(:result) do
        options = Couchbase::Options::Prepend.new(durability_level: :majority)
        binary_collection.prepend(doc_id, "bar", options)
      end

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the document has the correct content" do
        options = Couchbase::Options::Get.new(transcoder: Couchbase::RawBinaryTranscoder.new)
        expect(collection.get(doc_id, options).content).to eq("barfoo")
      end
    end
  end
end
