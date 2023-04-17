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

require "couchbase/raw_string_transcoder"
require "couchbase/transcoder_flags"
require "couchbase/errors"

class RawStringTranscoderTest < Minitest::Test
  include Couchbase::TestUtilities

  def setup
    @transcoder = Couchbase::RawStringTranscoder.new
  end

  def test_encode_string
    document = "foo"
    encoded, flag = @transcoder.encode(document)

    assert_equal 4, flag >> 24
    assert_equal "foo", encoded
  end

  def test_encode_binary
    document = "\x00\xff"

    assert_raises(Couchbase::Error::EncodingFailure) { @transcoder.encode(document) }
  end

  def test_encode_number
    document = 42

    assert_raises(Couchbase::Error::EncodingFailure) { @transcoder.encode(document) }
  end

  def test_decode
    blob = "foo"
    flag = Couchbase::TranscoderFlags.new(format: :string).encode

    decoded = @transcoder.decode(blob, flag)

    assert_equal "foo", decoded
  end

  def test_decode_invalid_flag
    blob = "\xff\x00"
    flag = Couchbase::TranscoderFlags.new(format: :binary).encode

    assert_raises(Couchbase::Error::DecodingFailure) { @transcoder.decode(blob, flag) }
  end
end
