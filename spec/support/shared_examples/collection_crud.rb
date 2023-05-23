require "rspec"

RSpec.shared_examples "collection crud operations" do
  let(:sample_content) do
    {"value" => 42}
  end

  let(:updated_content) do
    {"updated value" => 100}
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

  describe "#get" do
    context "when the document exists" do
      let(:doc_id) { upsert_sample_document }
      let(:result) { collection.get(doc_id) }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the result has the correct content" do
        expect(result.content).to eq(sample_content)
      end
    end

    context "when the document does not exist" do
      let(:doc_id) { uniq_id(:does_not_exist) }

      it "raises DocumentNotFound error" do
        expect { collection.get(doc_id) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "with expiry" do
      let(:doc_id) { upsert_sample_document(options: Couchbase::Options::Upsert.new(expiry: 60)) }
      let(:result) { collection.get(doc_id, Couchbase::Options::Get.new(with_expiry: true)) }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the result has the correct content" do
        expect(result.content).to eq(sample_content)
      end

      it "returns the expiry time" do
        expect(result.expiry_time).to be_a(Time)
      end

      it "the expiry time is in the future" do
        expect(result.expiry_time).to be > Time.now
      end
    end

    context "with projection" do
      def projected_get(doc_id, paths, options = Couchbase::Options::Get.new)
        options.project(paths)
        collection.get(doc_id, options)
      end

      context "with a single path" do
        let(:person) { load_json_test_dataset("projection_doc") }
        let(:doc_id) { upsert_sample_document(name: :project_doc, content: person) }

        let(:test_cases) do
          test_cases = [
            {name: "string", project: "name",
             expected: {"name" => person["name"]}},

            {name: "int", project: "age",
             expected: {"age" => person["age"]}},

            {name: "array", project: "animals",
             expected: {"animals" => person["animals"]}},

            {name: "array-index1", project: "animals[0]",
             expected: {"animals" => [person["animals"][0]]}},

            {name: "array-index2", project: "animals[1]",
             expected: {"animals" => [person["animals"][1]]}},

            {name: "array-index3", project: "animals[2]",
             expected: {"animals" => [person["animals"][2]]}},

            {name: "full-object-field", project: "attributes",
             expected: {"attributes" => person["attributes"]}},

            {name: "nested-object-field1", project: "attributes.hair",
             expected: {"attributes" => {"hair" => person["attributes"]["hair"]}}},

            {name: "nested-object-field2", project: "attributes.dimensions",
             expected: {"attributes" => {"dimensions" => person["attributes"]["dimensions"]}}},

            {name: "nested-object-field3", project: "attributes.dimensions.height",
             expected: {"attributes" => {"dimensions" => {"height" => person["attributes"]["dimensions"]["height"]}}}},

            {name: "nested-object-field4", project: "attributes.dimensions.weight",
             expected: {"attributes" => {"dimensions" => {"weight" => person["attributes"]["dimensions"]["weight"]}}}},

            {name: "nested-object-field5", project: "attributes.hobbies",
             expected: {"attributes" => {"hobbies" => person["attributes"]["hobbies"]}}},

            {name: "nested-array-object-field1", project: "attributes.hobbies[0].type",
             expected: {"attributes" => {"hobbies" => [{"type" => person["attributes"]["hobbies"][0]["type"]}]}}},

            {name: "nested-array-object-field2", project: "attributes.hobbies[1].type",
             expected: {"attributes" => {"hobbies" => [{"type" => person["attributes"]["hobbies"][1]["type"]}]}}},

            {name: "nested-array-object-field3", project: "attributes.hobbies[0].name",
             expected: {"attributes" => {"hobbies" => [{"name" => person["attributes"]["hobbies"][0]["name"]}]}}},

            {name: "nested-array-object-field4", project: "attributes.hobbies[1].name",
             expected: {"attributes" => {"hobbies" => [{"name" => person["attributes"]["hobbies"][1]["name"]}]}}},

            {name: "nested-array-object-field5", project: "attributes.hobbies[1].details",
             expected: {"attributes" => {"hobbies" => [{"details" => person["attributes"]["hobbies"][1]["details"]}]}}},

            {name: "nested-array-object-nested-field1", project: "attributes.hobbies[1].details.location",
             expected: {"attributes" => {"hobbies" => [
               {"details" => {"location" => person["attributes"]["hobbies"][1]["details"]["location"]}},
             ]}}},

            {name: "nested-array-object-nested-nested-field1", project: "attributes.hobbies[1].details.location.lat",
             expected: {"attributes" => {"hobbies" => [
               {"details" => {"location" => {"lat" => person["attributes"]["hobbies"][1]["details"]["location"]["lat"]}}},
             ]}}},

            {name: "nested-array-object-nested-nested-field2", project: "attributes.hobbies[1].details.location.long",
             expected: {"attributes" => {"hobbies" => [
               {"details" => {"location" => {"long" => person["attributes"]["hobbies"][1]["details"]["location"]["long"]}}},
             ]}}},
          ]

          unless use_caves?
            test_cases |=
              [
                {name: "array-of-arrays-object", project: "tracking.locations[1][1].lat",
                 expected: {"tracking" => {"locations" => [[{"lat" => person["tracking"]["locations"][1][1]["lat"]}]]}}},

                {name: "array-of-arrays-native", project: "tracking.raw[1][1]",
                 expected: {"tracking" => {"raw" => [[person["tracking"]["raw"][1][1]]]}}},
              ]
          end

          test_cases
        end

        it "returns the correct content" do
          test_cases.each do |test_case|
            expect(projected_get(doc_id, test_case[:project]).content)
              .to eq(test_case[:expected]), "unexpected content for case #{test_case[:name]} with projections #{test_case[:project].inspect}"
          end
        end
      end

      context "with few paths" do
        let(:person) { load_json_test_dataset("projection_doc") }
        let(:doc_id) { upsert_sample_document(name: :project_doc, content: person) }

        let(:test_cases) do
          [
            {name: "simple", project: %w[name age animals],
             expected: {"name" => person["name"], "age" => person["age"], "animals" => person["animals"]}},

            {name: "array entries", project: %w[animals[1] animals[0]],
             expected: {"animals" => [person["animals"][1], person["animals"][0]]}},
          ]
        end

        it "returns the correct content" do
          test_cases.each do |test_case|
            expect(projected_get(doc_id, test_case[:project]).content)
              .to eq(test_case[:expected]), "unexpected content for case #{test_case[:name]} with projections #{test_case[:project].inspect}"
          end
        end
      end

      context "when preserve_array_indexes is enabled" do
        let(:person) { load_json_test_dataset("projection_doc") }
        let(:doc_id) { upsert_sample_document(name: :project_doc, content: person) }

        let(:test_cases) do
          [
            {name: "array entries", project: %w[animals[1] animals[0]],
             expected: {"animals" => [person["animals"][0], person["animals"][1]]}},
            {name: "with inner array", project: %w[attributes.hobbies[1].details.location.lat],
             expected: {"attributes" => {"hobbies" => [
               nil,
               {
                 "details" => {
                   "location" => {
                     "lat" => person["attributes"]["hobbies"][1]["details"]["location"]["lat"],
                   },
                 },
               },
             ]}}},
          ]
        end

        it "returns the correct content with the array indexes in the original order" do
          test_cases.each do |test_case|
            get_opts = Couchbase::Options::Get.new { |o| o.preserve_array_indexes = true }
            expect(projected_get(doc_id, test_case[:project], get_opts).content)
              .to eq(test_case[:expected]), "unexpected content for case #{test_case[:name]} with projections #{test_case[:project].inspect}"
          end
        end
      end

      context "when projecting on 17 paths" do
        let(:doc) do
          (1..18).each_with_object({}) do |n, obj|
            obj["field#{n}"] = n
          end
        end

        let(:doc_id) { upsert_sample_document(name: :project_too_many_fields, content: doc) }

        let(:result) do
          projected_get(doc_id, (1..17).map { |n| "field#{n}" })
        end

        it "returns the correct content" do
          expect(result.content).to eq((1..17).each_with_object({}) { |n, obj| obj["field#{n}"] = n })
        end
      end

      context "when projecting on 16 paths with expiry" do
        let(:doc) do
          (1..18).each_with_object({}) do |n, obj|
            obj["field#{n}"] = n
          end
        end

        let(:doc_id) do
          opts = Couchbase::Options::Upsert.new(expiry: 60)
          upsert_sample_document(name: :project_too_many_fields, content: doc, options: opts)
        end

        let(:result) do
          opts = Couchbase::Options::Get.new { |o| o.with_expiry = true }
          projected_get(doc_id, (1..16).map { |n| "field#{n}" }, opts)
        end

        it "returns the correct content" do
          expect(result.content).to eq((1..16).each_with_object({}) { |n, obj| obj["field#{n}"] = n })
        end

        it "returns the expiry time" do
          expect(result.expiry_time).to be_a(Time)
        end

        it "the expiry time is in the future" do
          expect(result.expiry_time).to be > Time.now
        end
      end

      context "when projecting on a path that does not exist" do
        let(:person) { load_json_test_dataset("projection_doc") }
        let(:doc_id) { upsert_sample_document(name: :project_doc, content: person) }

        it "the content is empty if there are no other paths" do
          result = projected_get(doc_id, "this_field_does_not_exist")
          expect(result.content).to be_empty
        end

        it "the content includes the other paths that exist" do
          result = projected_get(doc_id, %w[this_field_does_not_exist age attributes.hair])
          expect(result.content).to eq({"age" => 26, "attributes" => {"hair" => "brown"}})
        end
      end
    end
  end

  describe "#upsert" do
    context "when the document already exists" do
      let(:doc_id) { upsert_sample_document(name: :foo) }
      let!(:result) { collection.upsert(doc_id, updated_content) }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the content of the document has been updated" do
        expect(collection.get(doc_id).content).to eq(updated_content)
      end
    end

    context "when the document does not currently exist" do
      let(:doc_id) { uniq_id(:foo) }
      let!(:result) { collection.upsert(doc_id, sample_content) }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the document has been inserted and has the correct content" do
        expect(collection.get(doc_id).content).to eq(sample_content)
      end
    end

    context "with preserve expiry enabled" do
      def get_expiry(id)
        options = Couchbase::Options::Get.new(with_expiry: true)
        collection.get(id, options).expiry_time
      end

      let(:doc_id) { uniq_id(:foo) }

      let!(:old_cas) do
        options = Couchbase::Options::Upsert.new(expiry: 1)
        collection.upsert(doc_id, sample_content, options).cas
      end

      let!(:old_expiry) { get_expiry(doc_id) }

      let!(:new_cas) do
        options = Couchbase::Options::Upsert.new(expiry: 10, preserve_expiry: true)
        collection.upsert(doc_id, updated_content, options).cas
      end

      let!(:new_expiry) { get_expiry(doc_id) }

      it "does not change the expiry" do
        expect(new_expiry).to eq(old_expiry)
      end

      it "updates the CAS" do
        expect(new_cas).not_to eq(old_cas)
      end

      it "the document can no longer be retrieved after its expiry" do
        sleep(2)
        expect { collection.get(doc_id) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end

  describe "#insert" do
    context "when the document does not currently exist" do
      let(:doc_id) { uniq_id(:foo) }
      let!(:result) { collection.insert(doc_id, sample_content) }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the result has success set to true" do
        expect(result.success?).to be true
      end

      it "the document has been inserted and has the correct content" do
        expect(collection.get(doc_id).content).to eq(sample_content)
      end
    end

    context "when the document already exists" do
      let!(:doc_id) { upsert_sample_document }

      it "raises DocumentExists error" do
        expect { collection.insert(doc_id, sample_content) }.to raise_error(Couchbase::Error::DocumentExists)
      end
    end
  end

  describe "#replace" do
    context "when the document exists" do
      let(:doc_id) { upsert_sample_document }
      let!(:result) { collection.replace(doc_id, updated_content) }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the result has success set to true" do
        expect(result.success?).to be true
      end

      it "the document has been replaced and has the correct content" do
        expect(collection.get(doc_id).content).to eq(updated_content)
      end
    end

    context "when the document does not exist" do
      let(:doc_id) { uniq_id(:foo) }

      it "raises DocumentNotFound error" do
        expect { collection.replace(doc_id, updated_content) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end

  describe "#remove" do
    context "when the document exists" do
      let(:doc_id) { upsert_sample_document }
      let!(:result) { collection.remove(doc_id) }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the document no longer exists" do
        expect { collection.get(doc_id) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document does not exist" do
      let(:doc_id) { uniq_id(:foo) }

      it "raises DocumentNotFound error" do
        expect { collection.remove(doc_id) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end

  describe "#exists" do
    context "when the document exists" do
      let(:doc_id) { upsert_sample_document }
      let(:result) { collection.exists(doc_id) }

      it "the result has non-zero CAS" do
        expect(result.cas).not_to be_zero
      end

      it "the `exists` field in the result is set to true" do
        expect(result.exists?).to be true
      end
    end

    context "when the document does not exist" do
      let(:doc_id) { uniq_id(:foo) }
      let!(:result) { collection.exists(doc_id) }

      it "the result has zero CAS" do
        expect(result.cas).to be_zero
      end

      it "the `exists` field in the result is set to false" do
        expect(result.exists?).to be false
      end
    end
  end

  describe "#get_and_lock" do
    context "when the document exists and is not locked" do
      let(:doc_id) { upsert_sample_document }
      let!(:get_result) { collection.get_and_lock(doc_id, 10) }

      it "fetches the document" do
        expect(get_result.cas).not_to be 0
      end

      it "protects the document from mutations" do
        expect { collection.remove(doc_id) }.to raise_error(Couchbase::Error::Timeout)
      end
    end

    context "when the document is locked" do
      let(:doc_id) { upsert_sample_document }

      before do
        collection.get_and_lock(doc_id, 10)
      end

      it "raises a Timeout error" do
        expect { collection.get_and_lock(doc_id, 10) }.to raise_error(Couchbase::Error::Timeout)
      end
    end

    context "when the document does not exist" do
      it "raises a DocumentNotFound error" do
        expect { collection.get_and_lock(unique_id(:foo), 10) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end

  describe "#unlock" do
    context "when the document exists and is not locked" do
      let(:doc_id) { upsert_sample_document }
      let(:cas) { collection.get(doc_id).cas }

      it "raises a Timeout error" do
        expect { collection.unlock(doc_id, cas) }.to raise_error(Couchbase::Error::Timeout)
      end
    end

    context "when the document is locked" do
      let(:doc_id) { upsert_sample_document }
      let(:cas) { collection.get_and_lock(doc_id, 10).cas }

      it "unlocks the document if the correct CAS is used" do
        collection.unlock(doc_id, cas)
        expect { collection.remove(doc_id) }.not_to raise_error
      end
    end

    context "when the document does not exist" do
      it "raises a DocumentNotFound error" do
        expect { collection.unlock(unique_id(:foo), 42) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end

  describe "#touch" do
    context "when the document exists" do
      let(:doc_id) { upsert_sample_document }

      it "sets the expiration" do
        collection.touch(doc_id, 1)
        sleep(5)
        expect { collection.get(doc_id) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end

    context "when the document does not exist" do
      it "raises a DocumentNotFound error" do
        expect { collection.touch(unique_id(:foo), 2) }.to raise_error(Couchbase::Error::DocumentNotFound)
      end
    end
  end
end
