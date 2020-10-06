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
  class SubdocTest < BaseTest
    def setup
      options = Cluster::ClusterOptions.new
      options.authenticate(TEST_USERNAME, TEST_PASSWORD)
      @cluster = Cluster.connect(TEST_CONNECTION_STRING, options)
      @bucket = @cluster.bucket(TEST_BUCKET)
      @collection = @bucket.default_collection
    end

    def teardown
      @cluster.disconnect if defined? @cluster
    end

    def test_mutate_in_increment
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)

      @collection.mutate_in(
        doc_id,
        [
          MutateInSpec.increment("value", 4),
        ]
      )

      expected = {"value" => 46}
      res = @collection.get(doc_id)
      assert_equal expected, res.content
    end

    def test_no_commands
      doc_id = uniq_id(:foo)
      assert_raises(ArgumentError) do
        @collection.mutate_in(doc_id, [])
      end
    end

    def test_load_primitives
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar", "num" => 1234})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.get("foo"),
                                    LookupInSpec.get("num"),
                                  ])

      assert_equal("bar", res.content(0))
      assert_equal(1234, res.content(1))

      assert res.exists?(0)
      assert res.exists?(1)
      refute res.exists?(2)

      assert res.cas && res.cas != 0
    end

    def test_load_object_and_array
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {
        "obj" => {},
        "arr" => [],
      })

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.get("obj"),
                                    LookupInSpec.get("arr"),
                                  ])

      assert res.exists?(0)
      assert res.exists?(1)

      assert_equal({}, res.content(0))
      assert_equal([], res.content(1))

      assert res.cas && res.cas != 0
    end

    def test_insert_primitive
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.insert("foo", "bar"),
                                  ])
      assert res.cas && res.cas != 0
      assert_equal({"foo" => "bar"}, @collection.get(doc_id).content)
    end

    def test_path_does_not_exist_single
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.get("does_not_exist"),
                                  ])
      assert_raises(Error::PathNotFound) do
        res.content(0)
      end
    end

    def test_path_does_not_exist_multi
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.get("does_not_exist"),
                                    LookupInSpec.get("foo"),
                                  ])
      refute res.exists?(0)
      assert res.exists?(1)
      assert_raises(Error::PathNotFound) do
        res.content(0)
      end
      assert_equal "bar", res.content(1)
    end

    def test_exists_single
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.exists("does_not_exist"),
                                  ])

      refute res.exists?(0)
      assert_raises Error::PathNotFound do
        res.content(0)
      end
    end

    def test_exists_multi
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.exists("does_not_exist"),
                                    LookupInSpec.get("foo"),
                                  ])

      refute res.exists?(0)
      assert_raises Error::PathNotFound do
        res.content(0)
      end

      assert res.exists?(1)
      assert_equal "bar", res.content(1)
    end

    def test_count
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => %w[hello world]})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.count("foo"),
                                  ])
      assert res.exists?(0)
      assert_equal 2, res.content(0)
    end

    def test_get_full_document
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.get(""),
                                  ])
      assert_equal({"foo" => "bar"}, res.content(0))
    end

    def test_upsert_full_document
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      options = Collection::MutateInOptions.new
      options.store_semantics = :upsert
      @collection.mutate_in(doc_id, [
                              # full document mutation should also carry XATTR spec
                              MutateInSpec.upsert("my_model.revision", 42).xattr.create_path,
                              MutateInSpec.replace("", {"bar" => "foo"}),
                            ], options)

      res = @collection.get(doc_id)
      assert_equal({"bar" => "foo"}, res.content)
    end

    def test_insert_full_document
      doc_id = uniq_id(:foo)

      options = Collection::MutateInOptions.new
      options.store_semantics = :insert
      @collection.mutate_in(doc_id, [
                              # full document mutation should also carry XATTR spec
                              MutateInSpec.upsert("my_model.revision", 42).xattr.create_path,
                              MutateInSpec.replace("", {"bar" => "foo"}),
                            ], options)

      res = @collection.get(doc_id)
      assert_equal({"bar" => "foo"}, res.content)
    end

    def test_counter_multi
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {
        "mutated" => 0,
        "body" => "",
        "first_name" => "James",
        "age" => 0,
      })

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.upsert("addr", {
                                      "state" => "NV",
                                      "pincode" => 7,
                                      "city" => "Chicago",
                                    }),
                                    MutateInSpec.increment("mutated", 1),
                                    MutateInSpec.upsert("name", {
                                      "last" => "",
                                      "first" => "James",
                                    }),
                                  ])

      assert_equal 1, res.content(1)
    end

    def test_counter_single
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {
        "mutated" => 0,
        "body" => "",
        "first_name" => "James",
        "age" => 0,
      })

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.increment("mutated", 1),
                                  ])

      assert_equal 1, res.content(0)
    end

    def test_subdoc_cas_with_durability
      skip("The server does not support sync replication (#{TEST_SERVER_VERSION}") unless TEST_SERVER_VERSION.supports_sync_replication?

      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"mutated" => 0})

      res = @collection.get(doc_id)

      error_count = 0
      2.times do
        options = Collection::MutateInOptions.new
        options.durability_level = :majority
        options.cas = res.cas
        @collection.mutate_in(doc_id, [
                                MutateInSpec.upsert("mutated", 1),
                              ], options)
      rescue Error::CasMismatch
        error_count += 1
      end

      assert_equal 1, error_count
    end

    def test_macros
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.get(:document).xattr,
                                    LookupInSpec.get(:cas).xattr,
                                    LookupInSpec.get(:is_deleted).xattr,
                                    LookupInSpec.get(:sequence_number).xattr,
                                    LookupInSpec.get(:value_size_bytes).xattr,
                                    LookupInSpec.get(:expiry_time).xattr,

                                  ])

      assert_kind_of Hash, res.content(0)
      assert_kind_of String, res.content(1) # HEX encoded CAS
      assert_equal res.content(1).sub(/^0x0*/, ''), res.content(1).to_i(16).to_s(16)
      assert_equal false, res.content(2) # rubocop:disable Minitest/RefuteFalse
      assert_kind_of String, res.content(3) # HEX encoded sequence number
      assert_equal res.content(3).sub(/^0x0*/, ''), res.content(3).to_i(16).to_s(16)
      assert_equal JSON.generate({}).size, res.content(4)
      assert_kind_of Integer, res.content(5)
    end

    def test_mad_hatter_macros
      skip("The server does not support MadHatter macros (#{TEST_SERVER_VERSION})") unless TEST_SERVER_VERSION.mad_hatter?

      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      res = @collection.lookup_in(doc_id, [
                                    LookupInSpec.get(:revision_id).xattr,
                                  ])

      assert_kind_of String, res.content(0)
    end

    def test_insert_string
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.insert("foo2", "bar2"),
                            ])

      assert_equal({"foo2" => "bar2"}, @collection.get(doc_id).content)
    end

    def test_remove
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.remove("foo"),
                            ])

      assert_equal({}, @collection.get(doc_id).content)
    end

    def test_insert_string_already_there
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      assert_raises(Error::PathExists) do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.insert("foo", "bar2"),
                              ])
      end
    end

    def test_insert_bool
      doc_id = uniq_id(:foo)

      cas = @collection.upsert(doc_id, {}).cas

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.insert("foo", false),
                                  ])
      assert_kind_of Integer, res.cas
      refute_equal cas, res.cas
      assert_equal false, @collection.get(doc_id).content["foo"] # rubocop:disable Minitest/RefuteFalse
    end

    def test_insert_int
      doc_id = uniq_id(:foo)

      cas = @collection.upsert(doc_id, {}).cas

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.insert("foo", 42),
                                  ])
      assert_kind_of Integer, res.cas
      refute_equal cas, res.cas
      assert_equal 42, @collection.get(doc_id).content["foo"]
    end

    def test_insert_double
      doc_id = uniq_id(:foo)

      cas = @collection.upsert(doc_id, {}).cas

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.insert("foo", 13.8),
                                  ])
      assert_kind_of Integer, res.cas
      refute_equal cas, res.cas
      assert_in_delta 13.8, @collection.get(doc_id).content["foo"]
    end

    def test_replace_string
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.replace("foo", "bar2"),
                            ])
      assert_equal "bar2", @collection.get(doc_id).content["foo"]
    end

    def test_replace_full_document
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.replace("", {"foo2" => "bar2"}),
                            ])
      assert_equal({"foo2" => "bar2"}, @collection.get(doc_id).content)
    end

    def test_replace_string_does_not_exist
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      assert_raises Error::PathNotFound do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.replace("foo", "bar2"),
                              ])
      end
    end

    def test_upsert_string
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => "bar"})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("foo", "bar2"),
                            ])
      assert_equal "bar2", @collection.get(doc_id).content["foo"]
    end

    def test_upsert_string_does_not_exist
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("foo", "bar2"),
                            ])
      assert_equal "bar2", @collection.get(doc_id).content["foo"]
    end

    def test_array_append
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => ["hello"]})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_append("foo", ["world"]),
                            ])
      assert_equal %w[hello world], @collection.get(doc_id).content["foo"]
    end

    def test_array_append_multi
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => ["hello"]})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_append("foo", %w[world mars]),
                            ])
      assert_equal %w[hello world mars], @collection.get(doc_id).content["foo"]
    end

    def test_array_append_list_string
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => ["hello"]})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_append("foo", ["world", %w[mars jupiter]]),
                            ])
      assert_equal ["hello", "world", %w[mars jupiter]], @collection.get(doc_id).content["foo"]
    end

    def test_array_prepend
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => ["hello"]})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_prepend("foo", ["world"]),
                            ])
      assert_equal %w[world hello], @collection.get(doc_id).content["foo"]
    end

    def test_array_prepend_multi
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => ["hello"]})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_prepend("foo", %w[world mars]),
                            ])
      assert_equal %w[world mars hello], @collection.get(doc_id).content["foo"]
    end

    def test_array_insert
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => %w[hello world]})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_insert("foo[1]", ["cruel"]),
                            ])
      assert_equal %w[hello cruel world], @collection.get(doc_id).content["foo"]
    end

    def test_array_insert_multi
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => %w[hello world]})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_insert("foo[1]", %w[cruel mars]),
                            ])
      assert_equal %w[hello cruel mars world], @collection.get(doc_id).content["foo"]
    end

    def test_array_insert_unique_does_not_exist
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => %w[hello world]})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_add_unique("foo", "cruel"),
                            ])
      assert_equal %w[hello world cruel], @collection.get(doc_id).content["foo"]
    end

    def test_array_insert_duplicated_unique_does_not_exist
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => %w[hello world]})

      assert_raises Error::PathExists do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.array_add_unique("foo", "cruel"),
                                MutateInSpec.array_add_unique("foo", "cruel"),
                              ])
      end
      assert_equal %w[hello world], @collection.get(doc_id).content["foo"]
    end

    def test_array_insert_unique_does_exist
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => %w[hello cruel world]})

      assert_raises Error::PathExists do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.array_add_unique("foo", "cruel"),
                              ])
      end
      assert_equal %w[hello cruel world], @collection.get(doc_id).content["foo"]
    end

    def test_counter_add
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => 10})

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.increment("foo", 5),
                                  ])
      assert_equal 15, res.content(0)
      assert_equal 15, @collection.get(doc_id).content["foo"]
    end

    def test_counter_minus
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => 10})

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.decrement("foo", 5),
                                  ])
      assert_equal 5, res.content(0)
      assert_equal 5, @collection.get(doc_id).content["foo"]
    end

    def test_insert_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.insert("x.foo", "bar2").xattr.create_path,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => "bar2"}, res.content(0))
      assert_equal({}, @collection.get(doc_id).content)
    end

    def test_remove_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => "bar"}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.remove("x.foo").xattr,
                            ])

      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({}, res.content(0))
    end

    def test_remove_xattr_does_not_exist
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      assert_raises(Error::PathNotFound) do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.remove("x.foo").xattr,
                              ])
      end
    end

    def test_insert_string_already_there_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => "bar"}).xattr,
                            ])

      assert_raises(Error::PathExists) do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.insert("x.foo", "bar2").xattr,
                              ])
      end
    end

    def test_replace_string_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => "bar"}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.replace("x.foo", "bar2").xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => "bar2"}, res.content(0))
    end

    def test_replace_string_does_not_exist_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => "bar"}).xattr,
                            ])

      assert_raises(Error::PathNotFound) do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.replace("x.foo2", "bar2").xattr,
                              ])
      end
    end

    def test_upsert_string_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => "bar"}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x.foo", "bar2").xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => "bar2"}, res.content(0))
    end

    def test_upsert_string_does_not_exist_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => "bar"}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x.foo2", "bar2").xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => "bar", "foo2" => "bar2"}, res.content(0))
    end

    def test_array_append_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => ["hello"]}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_append("x.foo", ["world"]).xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => %w[hello world]}, res.content(0))
    end

    def test_array_prepend_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => ["hello"]}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_prepend("x.foo", ["world"]).xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => %w[world hello]}, res.content(0))
    end

    def test_array_insert_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => %w[hello world]}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_insert("x.foo[1]", ["cruel"]).xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => %w[hello cruel world]}, res.content(0))
    end

    def test_array_add_unique_does_not_exist_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => %w[hello world]}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_add_unique("x.foo", "cruel").xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => %w[hello world cruel]}, res.content(0))
    end

    def test_array_add_unique_does_exist_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => %w[hello cruel world]}).xattr,
                            ])

      assert_raises Error::PathExists do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.array_add_unique("x.foo", "cruel").xattr,
                              ])
      end
    end

    def test_counter_add_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => 10}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.increment("x.foo", 5).xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => 15}, res.content(0))
    end

    def test_counter_minus_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => 10}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.decrement("x.foo", 5).xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => 5}, res.content(0))
    end

    def test_counter_add_xattr_bad_field_type
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => "not_a_number"}).xattr,
                            ])

      assert_raises Error::PathMismatch do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.increment("x.foo", 5).xattr,
                              ])
      end
    end

    def test_xattr_ops_are_reordered
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => 10}).xattr,
                            ])

      res = @collection.mutate_in(doc_id, [
                                    MutateInSpec.insert("foo2", "bar2"),
                                    MutateInSpec.increment("x.foo", 5).xattr,
                                  ])
      assert_equal 15, res.content(1)
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_equal({"foo" => 15}, res.content(0))
    end

    def test_insert_expand_macro_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.insert("x.foo", :cas).xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_kind_of String, res.content(0)["foo"] # HEX encoded sequence number
      assert_equal res.content(0)["foo"].sub(/^0x0*/, ''), res.content(0)["foo"].to_i(16).to_s(16)
    end

    def test_upsert_expand_macro_xattr
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x.foo", :cas).xattr,
                            ])
      res = @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr])
      assert_kind_of String, res.content(0)["foo"] # HEX encoded sequence number
      assert_equal res.content(0)["foo"].sub(/^0x0*/, ''), res.content(0)["foo"].to_i(16).to_s(16)
    end

    def test_insert_xattr_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.insert("x.foo.baz", "bar2").xattr.create_path,
                            ])
      assert_equal({"foo" => {"baz" => "bar2"}},
                   @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr]).content(0))
    end

    def test_insert_string_already_there_xattr_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => {"baz" => "bar"}}).xattr,
                            ])

      assert_raises Error::PathExists do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.insert("x.foo.baz", "bar2").xattr.create_path,
                              ])
      end
    end

    def test_upsert_string_xattr_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x", {"foo" => {"baz" => "bar"}}).xattr,
                            ])

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x.foo.baz", "bar2").xattr.create_path,
                            ])
      assert_equal({"foo" => {"baz" => "bar2"}},
                   @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr]).content(0))
    end

    def test_upsert_string_does_not_exist_xattr_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("x.foo.baz", "bar2").xattr.create_path,
                            ])
      assert_equal({"foo" => {"baz" => "bar2"}},
                   @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr]).content(0))
    end

    def test_array_append_xattr_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_append("x.foo", ["world"]).xattr.create_path,
                            ])
      assert_equal({"foo" => ["world"]},
                   @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr]).content(0))
    end

    def test_array_prepend_xattr_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_prepend("x.foo", ["world"]).xattr.create_path,
                            ])
      assert_equal({"foo" => ["world"]},
                   @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr]).content(0))
    end

    def test_counter_add_xattr_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.increment("x.foo", 5).xattr.create_path,
                            ])
      assert_equal({"foo" => 5},
                   @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr]).content(0))
    end

    def test_counter_minus_xattr_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.decrement("x.foo", 3).xattr.create_path,
                            ])
      assert_equal({"foo" => -3},
                   @collection.lookup_in(doc_id, [LookupInSpec.get("x").xattr]).content(0))
    end

    def test_insert_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.insert("foo.baz", "bar2").create_path,
                            ])
      assert_equal({"foo" => {"baz" => "bar2"}}, @collection.get(doc_id).content)
    end

    def test_insert_string_already_there_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => {"baz" => "bar"}})

      assert_raises Error::PathExists do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.insert("foo.baz", "bar2").create_path,
                              ])
      end
    end

    def test_upsert_string_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo" => {"baz" => "bar"}})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("foo.baz", "bar2").create_path,
                            ])
      assert_equal({"foo" => {"baz" => "bar2"}}, @collection.get(doc_id).content)
    end

    def test_upsert_string_does_not_exist_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("foo.baz", "bar2").create_path,
                            ])
      assert_equal({"foo" => {"baz" => "bar2"}}, @collection.get(doc_id).content)
    end

    def test_array_append_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_append("foo", ["world"]).create_path,
                            ])
      assert_equal({"foo" => ["world"]}, @collection.get(doc_id).content)
    end

    def test_array_prepend_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.array_prepend("foo", ["world"]).create_path,
                            ])
      assert_equal({"foo" => ["world"]}, @collection.get(doc_id).content)
    end

    def test_counter_add_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.increment("foo", 5).create_path,
                            ])
      assert_equal({"foo" => 5}, @collection.get(doc_id).content)
    end

    def test_counter_minus_create_path
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.decrement("foo", 3).create_path,
                            ])
      assert_equal({"foo" => -3}, @collection.get(doc_id).content)
    end

    def test_expiration
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"hello" => "world"})

      options = Collection::MutateInOptions.new
      options.expiry = 10
      @collection.mutate_in(doc_id, [
                              MutateInSpec.insert("foo2", "bar2"),
                            ], options)

      options = Collection::GetOptions.new
      options.with_expiry = true
      res = @collection.get(doc_id, options)
      assert_kind_of Time, res.expiry_time
      assert res.expiry_time > Time.now
    end

    def test_more_than_16_entries
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"hello" => "world"})

      assert_raises Error::InvalidArgument do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.insert("foo1", "bar1"),
                                MutateInSpec.insert("foo2", "bar2"),
                                MutateInSpec.insert("foo3", "bar3"),
                                MutateInSpec.insert("foo4", "bar4"),
                                MutateInSpec.insert("foo5", "bar5"),
                                MutateInSpec.insert("foo6", "bar6"),
                                MutateInSpec.insert("foo7", "bar7"),
                                MutateInSpec.insert("foo8", "bar8"),
                                MutateInSpec.insert("foo9", "bar9"),
                                MutateInSpec.insert("foo10", "bar10"),
                                MutateInSpec.insert("foo11", "bar11"),
                                MutateInSpec.insert("foo12", "bar12"),
                                MutateInSpec.insert("foo13", "bar13"),
                                MutateInSpec.insert("foo14", "bar14"),
                                MutateInSpec.insert("foo15", "bar15"),
                                MutateInSpec.insert("foo16", "bar16"),
                                MutateInSpec.insert("foo17", "bar17"),
                              ])
      end
    end

    def test_two_commands_succeed
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"hello" => "world"})

      @collection.mutate_in(doc_id, [
                              MutateInSpec.insert("foo1", "bar1"),
                              MutateInSpec.insert("foo2", "bar2"),
                            ])

      assert_equal({"hello" => "world", "foo1" => "bar1", "foo2" => "bar2"},
                   @collection.get(doc_id).content)
    end

    def test_two_commands_one_fails
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, {"foo1" => "bar_orig_1"})

      assert_raises Error::PathExists do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.insert("foo0", "bar0"),
                                MutateInSpec.insert("foo1", "bar1"),
                              ])
      end

      assert_equal({"foo1" => "bar_orig_1"}, @collection.get(doc_id).content)
    end

    def test_multiple_xattr_keys_should_fail
      doc_id = uniq_id(:foo)

      options = Collection::MutateInOptions.new
      options.store_semantics = :upsert
      assert_raises Error::XattrInvalidKeyCombo do
        @collection.mutate_in(doc_id, [
                                MutateInSpec.increment("count", 1).xattr.create_path,
                                MutateInSpec.insert("logs", "bar1").xattr.create_path,
                              ], options)
      end
    end

    def test_expiration_with_document_flags_should_not_fail
      doc_id = uniq_id(:foo)

      options = Collection::MutateInOptions.new
      options.store_semantics = :upsert
      # options.expiry = 60 * 60 * 24
      @collection.mutate_in(doc_id, [
                              MutateInSpec.upsert("a", "b"),
                              MutateInSpec.upsert("c", "d"),
                            ], options)
    end

    def test_create_tombstones
      doc_id = uniq_id(:foo)

      options = Collection::MutateInOptions.new
      options.store_semantics = :upsert
      options.create_as_deleted = true
      unless TEST_SERVER_VERSION.supports_create_as_deleted?
        assert_raises Error::UnsupportedOperation do
          @collection.mutate_in(doc_id, [
            MutateInSpec.upsert("meta.field", "b").xattr.create_path,
          ], options)
        end
        return
      end
      res = @collection.mutate_in(doc_id, [
        MutateInSpec.upsert("meta.field", "b").xattr.create_path,
      ], options)
      assert res.deleted?, "the document should be marked as 'deleted'"

      assert_raises(Error::DocumentNotFound) do
        @collection.get(doc_id)
      end

      options = Collection::LookupInOptions.new
      options.access_deleted = true
      res = @collection.lookup_in(doc_id, [
        LookupInSpec.get("meta").xattr,
      ], options)
      assert_equal({"field" => "b"}, res.content(0))
      assert res.deleted?, "the document should be marked as 'deleted'"
    end
  end
end
