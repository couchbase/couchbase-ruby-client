#    Copyright 2020 Couchbase, Inc.
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

require_relative "test_helper"

module Couchbase
  class CrudTest < BaseTest
    def setup
      options = Cluster::ClusterOptions.new
      options.authenticate(TEST_USERNAME, TEST_PASSWORD)
      @cluster = Cluster.connect(TEST_CONNECTION_STRING, options)
      @collection = @cluster.bucket("default").default_collection
    end

    def teardown
      @cluster.disconnect
    end

    def uniq_id(name)
      "#{name}_#{Time.now.to_f}"
    end

    def test_create_documents
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)
      res = @collection.get(doc_id)
      assert_equal document, res.content
    end

    def test_removes_documents
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)

      @collection.remove(doc_id)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_touch_sets_expiration
      document = {"value" => 42}
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, document)

      @collection.touch(doc_id, 1)

      sleep(2)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_get_can_also_set_expiration
      document = {"value" => 42}
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, document)

      res = @collection.get_and_touch(doc_id, 1)
      assert_equal 42, res.content["value"]

      sleep(2)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_exists_allows_to_check_document_existence
      doc_id = uniq_id(:foo)

      res = @collection.exists(doc_id)
      refute res.exists?

      document = {"value" => 42}
      res = @collection.upsert(doc_id, document)
      cas = res.cas

      res = @collection.exists(doc_id)
      assert res.exists?
      assert_equal cas, res.cas
    end

    def test_get_and_lock_protects_document_from_mutations
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)

      res = @collection.get_and_lock(doc_id, 1)
      cas = res.cas

      document["value"] += 1
      assert_raises(Couchbase::Error::DocumentLocked) do
        @collection.upsert(doc_id, document)
      end

      @collection.unlock(doc_id, cas)

      @collection.upsert(doc_id, document)
    end

    def test_insert_fails_when_document_exists_already
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.insert(doc_id, document)

      assert_raises(Couchbase::Error::DocumentExists) do
        @collection.insert(doc_id, document)
      end
    end

    def test_replace_fails_when_document_does_not_exist_yet
      doc_id = uniq_id(:foo)
      document = {"value" => 42}

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.replace(doc_id, document)
      end

      @collection.upsert(doc_id, document)

      document["value"] = 43
      @collection.replace(doc_id, document)

      res = @collection.get(doc_id)

      assert_equal 43, res.content["value"]
    end

    def test_replace_supports_optimistic_locking
      doc_id = uniq_id(:foo)
      document = {"value" => 42}

      res = @collection.upsert(doc_id, document)
      cas = res.cas

      options = Collection::ReplaceOptions.new
      options.cas = cas + 1 # incorrect CAS

      document["value"] = 43
      assert_raises(Couchbase::Error::CasMismatch) do
        @collection.replace(doc_id, document, options)
      end

      options.cas = cas # correct CAS
      @collection.replace(doc_id, document, options)

      res = @collection.get(doc_id)

      assert_equal 43, res.content["value"]
    end

    class BinaryTranscoder
      def encode(blob)
        [blob, 0]
      end

      def decode(blob, _flags)
        blob
      end
    end

    def test_increments_and_decrements_existing_binary_document
      doc_id = uniq_id(:foo)
      document = "42"

      options = Collection::UpsertOptions.new
      options.transcoder = BinaryTranscoder.new
      @collection.upsert(doc_id, document, options)

      res = @collection.binary.increment(doc_id)
      assert_equal 43, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "43", res.content

      res = @collection.binary.decrement(doc_id)
      assert_equal 42, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "42", res.content
    end

    def test_fails_to_increment_and_decrement_missing_document
      doc_id = uniq_id(:foo)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.binary.increment(doc_id)
      end

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.binary.decrement(doc_id)
      end
    end

    def test_increment_and_decrement_can_initialize_document
      doc_id = uniq_id(:foo)

      options = BinaryCollection::IncrementOptions.new
      options.initial = 42
      res = @collection.binary.increment(doc_id, options)
      assert_equal 42, res.content
      res = @collection.binary.increment(doc_id, options)
      assert_equal 43, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "43", res.content

      doc_id = uniq_id(:bar)
      options = BinaryCollection::DecrementOptions.new
      options.initial = 142
      res = @collection.binary.decrement(doc_id, options)
      assert_equal 142, res.content
      res = @collection.binary.decrement(doc_id, options)
      assert_equal 141, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "141", res.content
    end

    def test_increment_and_decrement_can_use_custom_delta
      doc_id = uniq_id(:foo)

      options = BinaryCollection::IncrementOptions.new
      options.initial = 42
      options.delta = 50
      res = @collection.binary.increment(doc_id, options)
      assert_equal 42, res.content
      res = @collection.binary.increment(doc_id, options)
      assert_equal 92, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "92", res.content

      doc_id = uniq_id(:bar)
      options = BinaryCollection::DecrementOptions.new
      options.initial = 142
      options.delta = 20
      res = @collection.binary.decrement(doc_id, options)
      assert_equal 142, res.content
      res = @collection.binary.decrement(doc_id, options)
      assert_equal 122, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "122", res.content
    end

    def test_insert_bigger_document_and_get_full
      doc_id = uniq_id(:project_doc)
      person = load_json_test_dataset("projection_doc")

      res = @collection.upsert(doc_id, person)
      assert_kind_of Integer, res.cas
      refute_equal 0, res.cas
      cas = res.cas

      res = @collection.get(doc_id)
      assert_equal cas, res.cas
      assert_equal person, res.content
    end

    def test_error_not_existent
      doc_id = uniq_id(:does_not_exist)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_error_double_insert
      doc_id = uniq_id(:does_not_exist)

      @collection.insert(doc_id, "test")

      assert_raises(Couchbase::Error::DocumentExists) do
        @collection.insert(doc_id, "test")
      end
    end

    def test_error_insert_get_with_expiration
      doc_id = uniq_id(:expiry_doc)
      doc = load_json_test_dataset("beer_sample_single")

      options = Collection::InsertOptions.new
      options.expiration = 10
      res = @collection.insert(doc_id, doc, options)
      refute_equal 0, res.cas

      options = Collection::GetOptions.new
      options.with_expiration = true
      res = @collection.get(doc_id, options)

      assert_equal doc, res.content
      assert_kind_of Integer, res.expiration
      refute_equal 0, res.expiration
    end

    def test_insert_get_projection
      doc_id = uniq_id(:project_doc)
      person = load_json_test_dataset("projection_doc")

      res = @collection.upsert(doc_id, person)
      refute_equal 0, res.cas

      test_cases = [
          {name: "string", project: "name",
           expected:
               {
                   "name" => person["name"]
               }
          },

          {name: "int", project: "age",
           expected:
               {
                   "age" => person["age"]
               }
          },

          {name: "array", project: "animals",
           expected:
               {
                   "animals" => person["animals"]
               }
          },

          {name: "array-index1", project: "animals[0]",
           expected:
               {
                   "animals" => [
                       person["animals"][0]
                   ]
               }
          },

          {name: "array-index2", project: "animals[1]",
           expected:
               {
                   "animals" => [
                       person["animals"][1]
                   ]
               }
          },

          {name: "array-index3", project: "animals[2]",
           expected:
               {
                   "animals" => [
                       person["animals"][2]
                   ]
               }
          },

          {name: "full-object-field", project: "attributes",
           expected:
               {
                   "attributes" => person["attributes"]
               }
          },

          {name: "nested-object-field1", project: "attributes.hair",
           expected:
               {
                   "attributes" => {
                       "hair" => person["attributes"]["hair"]
                   }
               }
          },

          {name: "nested-object-field2", project: "attributes.dimensions",
           expected:
               {
                   "attributes" => {
                       "dimensions" => person["attributes"]["dimensions"]
                   }
               }
          },

          {name: "nested-object-field3", project: "attributes.dimensions.height",
           expected:
               {
                   "attributes" => {
                       "dimensions" => {
                           "height" => person["attributes"]["dimensions"]["height"]
                       }
                   }
               }
          },

          {name: "nested-object-field4", project: "attributes.dimensions.weight",
           expected:
               {
                   "attributes" => {
                       "dimensions" => {
                           "weight" => person["attributes"]["dimensions"]["weight"]
                       }
                   }
               }
          },

          {name: "nested-object-field5", project: "attributes.hobbies",
           expected:
               {
                   "attributes" => {
                       "hobbies" => person["attributes"]["hobbies"]
                   }
               }
          },

          {name: "nested-array-object-field1", project: "attributes.hobbies[0].type",
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           {
                               "type" => person["attributes"]["hobbies"][0]["type"]
                           }
                       ]
                   }
               }
          },

          {name: "nested-array-object-field2", project: "attributes.hobbies[1].type",
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           {
                               "type" => person["attributes"]["hobbies"][1]["type"]
                           }
                       ]
                   }
               }
          },

          {name: "nested-array-object-field3", project: "attributes.hobbies[0].name",
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           {
                               "name" => person["attributes"]["hobbies"][0]["name"]
                           }
                       ]
                   }
               }
          },

          {name: "nested-array-object-field4", project: "attributes.hobbies[1].name",
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           {
                               "name" => person["attributes"]["hobbies"][1]["name"]
                           }
                       ]
                   }
               }
          },

          {name: "nested-array-object-field5", project: "attributes.hobbies[1].details",
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           {
                               "details" => person["attributes"]["hobbies"][1]["details"]
                           }
                       ]
                   }
               }
          },

          {name: "nested-array-object-nested-field1", project: "attributes.hobbies[1].details.location",
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           {
                               "details" => {
                                   "location" => person["attributes"]["hobbies"][1]["details"]["location"]
                               }
                           }
                       ]
                   }
               }
          },

          {name: "nested-array-object-nested-nested-field1", project: "attributes.hobbies[1].details.location.lat",
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           {
                               "details" => {
                                   "location" => {
                                       "lat" => person["attributes"]["hobbies"][1]["details"]["location"]["lat"]
                                   }
                               }
                           }
                       ]
                   }
               }
          },

          {name: "nested-array-object-nested-nested-field2", project: "attributes.hobbies[1].details.location.long",
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           {
                               "details" => {
                                   "location" => {
                                       "long" => person["attributes"]["hobbies"][1]["details"]["location"]["long"]
                                   }
                               }
                           }
                       ]
                   }
               }
          },

          {name: "array-of-arrays-object", project: "tracking.locations[1][1].lat",
           expected:
               {
                   "tracking" => {
                       "locations" => [
                           [
                               {"lat" => person["tracking"]["locations"][1][1]["lat"]}
                           ]
                       ]
                   }
               }
          },

          {name: "array-of-arrays-native", project: "tracking.raw[1][1]",
           expected:
               {
                   "tracking" => {
                       "raw" => [
                           [
                               person["tracking"]["raw"][1][1]
                           ]
                       ]
                   }
               }
          },
      ]

      test_cases.each do |test_case|
        options = Collection::GetOptions.new
        options.project(test_case[:project])
        res = @collection.get(doc_id, options)
        assert_equal(test_case[:expected], res.content,
                     "unexpected content for case #{test_case[:name]} with projections #{test_case[:project].inspect}")
      end
    end

    def test_projection_few_paths
      doc_id = uniq_id(:project_doc)
      person = load_json_test_dataset("projection_doc")

      res = @collection.upsert(doc_id, person)
      refute_equal 0, res.cas

      test_cases = [
          {name: "simple", project: %w[name age animals],
           expected:
               {
                   "name" => person["name"],
                   "age" => person["age"],
                   "animals" => person["animals"]
               }
          },
          {name: "array entries", project: %w[animals[1] animals[0]],
           expected:
               {
                   "animals" => [
                       person["animals"][1],
                       person["animals"][0]
                   ]
               }
          },
      ]

      test_cases.each do |test_case|
        options = Collection::GetOptions.new
        options.project(*test_case[:project])
        res = @collection.get(doc_id, options)
        assert_equal(test_case[:expected], res.content,
                     "unexpected content for case #{test_case[:name]} with projections #{test_case[:project].inspect}")
      end
    end

    def test_projection_preserve_array_indexes
      doc_id = uniq_id(:project_doc)
      person = load_json_test_dataset("projection_doc")

      res = @collection.upsert(doc_id, person)
      refute_equal 0, res.cas

      test_cases = [
          {name: "array entries", project: %w[animals[1] animals[0]],
           expected:
               {
                   "animals" => [
                       person["animals"][0],
                       person["animals"][1]
                   ]
               }
          },
          {name: "with inner array", project: %w[attributes.hobbies[1].details.location.lat],
           expected:
               {
                   "attributes" => {
                       "hobbies" => [
                           nil,
                           {
                               "details" => {
                                   "location" => {
                                       "lat" => person["attributes"]["hobbies"][1]["details"]["location"]["lat"]
                                   }
                               }
                           }
                       ]
                   }
               }
          },
      ]

      test_cases.each do |test_case|
        options = Collection::GetOptions.new
        options.project(*test_case[:project])
        options.preserve_array_indexes = true
        res = @collection.get(doc_id, options)
        assert_equal(test_case[:expected], res.content,
                     "unexpected content for case #{test_case[:name]} with projections #{test_case[:project].inspect}")
      end
    end

    def test_insert_get_projection_18_fields
      doc_id = uniq_id(:project_too_many_fields)
      doc = (1..18).each_with_object({}) do |n, obj|
        obj["field#{n}"] = n
      end

      res = @collection.insert(doc_id, doc)
      refute_equal 0, res.cas

      options = Collection::GetOptions.new
      options.project((1..17).map {|n| "field#{n}"})
      res = @collection.get(doc_id, options)
      expected = (1..17).each_with_object({}) do |n, obj|
        obj["field#{n}"] = n
      end
      assert_equal(expected, res.content, "expected result do not include field18")
    end

    def test_upsert_get_projection_16_fields_and_expiry
      doc_id = uniq_id(:project_too_many_fields)
      doc = (1..18).each_with_object({}) do |n, obj|
        obj["field#{n}"] = n
      end

      options = Collection::UpsertOptions.new
      options.expiration = 60
      res = @collection.upsert(doc_id, doc, options)
      refute_equal 0, res.cas

      options = Collection::GetOptions.new
      options.project((1..16).map {|n| "field#{n}"})
      options.with_expiration = true
      res = @collection.get(doc_id, options)
      expected = (1..16).each_with_object({}) do |n, obj|
        obj["field#{n}"] = n
      end
      assert_equal(expected, res.content, "expected result do not include field17, field18")
      assert_kind_of(Integer, res.expiration)
      refute_equal(0, res.expiration)
    end

    def test_upsert_get_projection_missing_path
      doc_id = uniq_id(:project_doc)
      person = load_json_test_dataset("projection_doc")

      res = @collection.upsert(doc_id, person)
      refute_equal 0, res.cas

      options = Collection::GetOptions.new
      options.project("this_field_does_not_exist")
      assert_raises(Error::PathNotFound) do
        @collection.get(doc_id, options)
      end
    end
  end
end
