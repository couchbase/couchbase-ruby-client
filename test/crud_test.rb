#  Copyright 2020-2021 Couchbase, Inc.
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
  class CrudTest < Minitest::Test
    include TestUtilities

    def setup
      connect
      @bucket = @cluster.bucket(env.bucket)
      @collection = @bucket.default_collection
    end

    def teardown
      disconnect
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

    def test_reads_from_replica
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      options =
        if env.jenkins?
          Options::Upsert(persist_to: :active, replicate_to: :one)
        else
          Options::Upsert(persist_to: :active)
        end
      @collection.upsert(doc_id, document, options)

      res = @collection.get_any_replica(doc_id)

      assert_equal document, res.content
      assert_respond_to res, :replica?

      res = @collection.get_all_replicas(doc_id)

      refute_empty res
      res.each do |entry|
        assert_equal document, entry.content
        assert_respond_to entry, :replica?
      end
    end

    def test_touch_sets_expiration
      document = {"value" => 42}
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, document)

      @collection.touch(doc_id, 1)

      time_travel(2)

      assert_raises(Couchbase::Error::DocumentNotFound, "Document \"#{doc_id}\" should be expired") do
        @collection.get(doc_id)
      end
    end

    def test_get_can_also_set_expiration
      document = {"value" => 42}
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, document)

      res = @collection.get_and_touch(doc_id, 1)

      assert_equal 42, res.content["value"]

      time_travel(2)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_exists_allows_to_check_document_existence
      doc_id = uniq_id(:foo)

      res = @collection.exists(doc_id)

      refute_predicate res, :exists?

      document = {"value" => 42}
      res = @collection.upsert(doc_id, document)
      cas = res.cas

      res = @collection.exists(doc_id)

      assert_predicate res, :exists?
      assert_equal cas, res.cas

      @collection.remove(doc_id)

      res = @collection.exists(doc_id)

      refute_predicate res, :exists?
    end

    def test_get_and_lock_protects_document_from_mutations
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)

      res = @collection.get_and_lock(doc_id, 5)
      cas = res.cas

      document["value"] += 1
      assert_raises(Couchbase::Error::Timeout) do
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
      options.expiry = 10
      res = @collection.insert(doc_id, doc, options)

      refute_equal 0, res.cas

      options = Collection::GetOptions.new
      options.with_expiry = true
      res = @collection.get(doc_id, options)

      assert_equal doc, res.content
      assert_kind_of Time, res.expiry_time
      now = Time.now

      assert res.expiry_time >= now, "now: #{now} (#{now.to_i}), expiry_time: #{res.expiry_time} (#{res.expiry_time.to_i})"
    end

    def test_expiry_option_as_time_instance
      doc_id = uniq_id(:expiry_doc)
      doc = load_json_test_dataset("beer_sample_single")

      today = Time.now.round
      tomorrow = today + (24 * 60 * 60) # add one day
      res = @collection.insert(doc_id, doc, Options::Insert(expiry: tomorrow))

      refute_equal 0, res.cas

      res = @collection.get(doc_id, Options::Get(with_expiry: true))

      assert_equal doc, res.content
      assert_equal tomorrow, res.expiry_time
    end

    def test_integer_expiry_of_40_days_remains_relative
      doc_id = uniq_id(:expiry_doc)
      doc = load_json_test_dataset("beer_sample_single")

      forty_days_from_today = 40 * 24 * 60 * 60
      res = @collection.insert(doc_id, doc, Options::Insert(expiry: forty_days_from_today))

      refute_equal 0, res.cas

      res = @collection.get(doc_id, Options::Get(with_expiry: true))

      assert_equal doc, res.content
      assert_kind_of Time, res.expiry_time
      # check that expiration time is not greater than 2 seconds to the expected
      assert_in_delta (Time.now + forty_days_from_today).to_f, res.expiry_time.to_f, 2
    end

    def test_insert_get_projection
      doc_id = uniq_id(:project_doc)
      person = load_json_test_dataset("projection_doc")

      res = @collection.upsert(doc_id, person)

      refute_equal 0, res.cas

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
         expected: {"name" => person["name"], "age" => person["age"], "animals" => person["animals"]}},

        {name: "array entries", project: %w[animals[1] animals[0]],
         expected: {"animals" => [person["animals"][1], person["animals"][0]]}},
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
      options.project((1..17).map { |n| "field#{n}" })
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
      options.expiry = 60
      res = @collection.upsert(doc_id, doc, options)

      refute_equal 0, res.cas

      options = Collection::GetOptions.new
      options.project((1..16).map { |n| "field#{n}" })
      options.with_expiry = true
      res = @collection.get(doc_id, options)
      expected = (1..16).each_with_object({}) do |n, obj|
        obj["field#{n}"] = n
      end

      assert_equal(expected, res.content, "expected result do not include field17, field18")
      assert_kind_of(Time, res.expiry_time)
      assert(res.expiry_time > Time.now)
    end

    def test_upsert_get_projection_missing_path
      doc_id = uniq_id(:project_doc)
      person = load_json_test_dataset("projection_doc")

      res = @collection.upsert(doc_id, person)

      refute_equal 0, res.cas

      options = Collection::GetOptions.new
      options.project("this_field_does_not_exist")
      res = @collection.get(doc_id, options)

      assert_empty(res.content)

      options = Collection::GetOptions.new
      options.project("this_field_does_not_exist", "age", "attributes.hair")
      res = @collection.get(doc_id, options)

      assert_equal({"age" => 26, "attributes" => {"hair" => "brown"}}, res.content)
    end

    def __wait_for_collections_manifest(uid)
      hits = 5 # make sure that the manifest has distributed well enough
      while hits.positive?
        backend = @cluster.instance_variable_get(:@backend)
        manifest = backend.collections_manifest_get(@bucket.name, 10_000)
        if manifest[:uid] < uid
          time_travel(0.1)
          next
        end
        hits -= 1
      end
    end

    # Following test tests that if a collection is deleted and recreated midway through a set of operations then the
    # operations will still succeed due to the cid being refreshed under the hood.
    def test_collection_retry
      skip("The server does not support collections (#{env.server_version})") unless env.server_version.supports_collections?
      doc_id = uniq_id(:test_collection_retry)
      doc = load_json_test_dataset("beer_sample_single")

      collection_name = uniq_id(:collection).delete(".")[0, 30]

      manager = @bucket.collections

      spec = Management::CollectionSpec.new
      spec.scope_name = "_default"
      spec.name = collection_name
      ns_uid = manager.create_collection(spec)
      __wait_for_collections_manifest(ns_uid)

      collection = @bucket.collection(collection_name)

      # make sure we've connected to the collection
      options = Collection::UpsertOptions.new
      options.timeout = 15_000
      res = collection.upsert(doc_id, doc, options)

      refute_equal 0, res.cas

      # the following delete and create will recreate a collection with the same name but a different collection ID.
      ns_uid = manager.drop_collection(spec)
      __wait_for_collections_manifest(ns_uid)
      ns_uid = manager.create_collection(spec)
      __wait_for_collections_manifest(ns_uid)

      # we've wiped the collection so we need to recreate this doc
      # we know that this operation can take a bit longer than normal as collections take time to come online
      options = Collection::UpsertOptions.new
      options.timeout = 15_000
      res = collection.upsert(doc_id, doc, options)

      refute_equal 0, res.cas

      res = collection.get(doc_id)

      assert_equal doc, res.content
    end

    def test_append
      doc_id = uniq_id(:append)

      res = @collection.upsert(doc_id, "foo")

      refute_equal 0, res.cas

      res = @collection.binary.append(doc_id, "bar")

      refute_equal 0, res.cas

      res = @collection.get(doc_id, Options::Get(transcoder: nil))

      refute_equal "foobar", res.content
    end

    def test_prepend
      doc_id = uniq_id(:append)

      res = @collection.upsert(doc_id, "foo")

      refute_equal 0, res.cas

      res = @collection.binary.prepend(doc_id, "bar")

      refute_equal 0, res.cas

      res = @collection.get(doc_id, Options::Get(transcoder: nil))

      refute_equal "barfoo", res.content
    end

    def test_multi_ops
      doc_id1 = uniq_id(:foo)
      doc_id2 = uniq_id(:bar)

      res = @collection.upsert_multi([
                                       [doc_id1, {"foo" => 32}],
                                       [doc_id2, {"bar" => "bar42"}],
                                     ])

      assert_kind_of Array, res
      assert_equal 2, res.size
      assert_nil res[0].error
      assert_nil res[1].error
      assert_equal res[0].id, doc_id1
      assert_equal res[1].id, doc_id2

      cas2 = res[1].cas

      res = @collection.get_multi([doc_id1, doc_id2, uniq_id(:does_not_exist)])

      assert_kind_of Array, res
      assert_equal 3, res.size
      assert_nil res[0].error
      assert_nil res[1].error
      assert_equal res[0].id, doc_id1
      assert_equal res[1].id, doc_id2
      assert_kind_of Error::DocumentNotFound, res[2].error

      assert_equal({"foo" => 32}, res[0].content)
      assert_equal({"bar" => "bar42"}, res[1].content)
      assert_equal(cas2, res[1].cas)

      res = @collection.remove_multi([doc_id1, [doc_id2, cas2]])

      assert_kind_of Array, res
      assert_equal 2, res.size
      assert_nil res[0].error
      assert_nil res[1].error
      assert_equal res[0].id, doc_id1
      assert_equal res[1].id, doc_id2

      res = @collection.get_multi([doc_id1, doc_id2])

      assert_kind_of Array, res
      assert_equal 2, res.size
      assert_kind_of Error::DocumentNotFound, res[0].error
      assert_kind_of Error::DocumentNotFound, res[1].error
      assert_equal res[0].id, doc_id1
      assert_equal res[1].id, doc_id2
    end

    def test_massive_multi_ops
      # github often chokes on such a big batches (and raises Error::Timeout)
      num_keys = ENV.fetch("TEST_MASSIVE_MULTI_OPS_NUM_KEYS", nil) || 1_000
      keys = (0..num_keys).map { |idx| uniq_id("key_#{idx}") }

      res = @collection.upsert_multi(keys.map { |k| [k, {"value" => k}] })

      assert_kind_of Array, res
      assert_equal keys.size, res.size
      res.each_with_index do |r, i|
        assert_nil r.error, "upsert for #{keys[i].inspect} must not fail"
      end

      res = @collection.get_multi(keys)

      assert_kind_of Array, res
      assert_equal keys.size, res.size
      res.each_with_index do |r, i|
        assert_nil r.error, "upsert for #{keys[i].inspect} must not fail"
        assert_equal keys[i], r.content["value"]
      end
    end

    def test_preserve_expiry
      skip("#{name}: CAVES does not support preserve expiry") if use_caves?
      skip("The server does not support preserve expiry (#{env.server_version})") unless env.server_version.supports_preserve_expiry?

      doc_id = uniq_id(:foo)
      res = @collection.upsert(doc_id, {answer: 42}, Options::Upsert(expiry: 1))
      old_cas = res.cas

      refute_equal 0, old_cas

      res = @collection.get(doc_id, Options::Get(with_expiry: true))
      old_expiry = res.expiry_time

      res = @collection.upsert(doc_id, {answer: 43}, Options::Upsert(expiry: 100, preserve_expiry: true))

      refute_equal old_cas, res.cas

      res = @collection.get(doc_id, Options::Get(with_expiry: true))

      assert_equal old_expiry, res.expiry_time

      time_travel(2)
      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_legacy_durability
      doc_id = uniq_id(:foo)
      res = @collection.upsert(doc_id, {answer: 42}, Options::Upsert(persist_to: :active))
      old_cas = res.cas

      refute_equal 0, old_cas
    end
  end
end
