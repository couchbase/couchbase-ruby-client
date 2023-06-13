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

require "test_helper"

require "couchbase/utils/time"

require "active_support/core_ext/numeric/time"
require "active_support/duration"

class MyDuration
  def initialize(seconds)
    @value = seconds
  end

  def in_milliseconds
    @value * 1000
  end
end

class UtilsTimeTest < Minitest::Test
  def test_accepts_rails_duration
    duration = 42.seconds

    assert_kind_of ActiveSupport::Duration, duration
    assert_equal 42_000, Couchbase::Utils::Time.extract_duration(duration)
  end

  def test_accepts_duration_like_objects
    assert_equal 42_000, Couchbase::Utils::Time.extract_duration(MyDuration.new(42))
  end

  def test_accepts_floats
    duration = Couchbase::Utils::Time.extract_duration(0.42)

    assert_kind_of Integer, duration
    assert_equal 420, duration
  end

  def test_interpret_integer_as_a_milliseconds_literal
    assert_equal 42, Couchbase::Utils::Time.extract_duration(42)
  end
end
