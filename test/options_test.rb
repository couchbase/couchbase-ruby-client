# frozen_string_literal: true

#  Copyright 2026. Couchbase, Inc.
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

require "test_helper"

class OptionsTest < Minitest::Test
  def options_classes
    ObjectSpace.each_object(Class).select { |klass| klass < Couchbase::Options::Base }
  end

  def test_every_options_class_runs_its_block_once
    refute_empty options_classes

    options_classes.each do |klass|
      calls = 0
      klass.new { calls += 1 }

      assert_equal 1, calls, "#{klass.name}.new runs its block #{calls} times"
    end
  end

  def test_base_runs_its_block_once
    calls = 0
    Couchbase::Options::Base.new { calls += 1 }

    assert_equal 1, calls
  end

  def test_block_sees_subclass_defaults
    transcoders = []
    Couchbase::Options::Get.new { |options| transcoders << options.transcoder }

    assert_equal 1, transcoders.size
    assert_kind_of Couchbase::JsonTranscoder, transcoders.first
  end

  def test_block_can_call_builder_methods
    [Couchbase::Options::Analytics, Couchbase::Options::Query].each do |klass|
      options = klass.new { |o| o.raw("key", 1) }

      assert_equal({"key" => "1"}, options.to_backend[:raw_parameters], klass.name)
    end
  end
end
