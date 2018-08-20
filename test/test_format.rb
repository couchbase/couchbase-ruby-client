# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2018 Couchbase, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'yajl'
require File.join(__dir__, 'setup')

class TestFormat < MiniTest::Test
  ArbitraryClass = Struct.new(:name, :role)
  class SkinyClass < Struct.new(:name, :role)
    undef to_s rescue nil
    undef to_json rescue nil
  end

  def test_default_document_format
    orig_doc = {'name' => 'Twoflower', 'role' => 'The tourist'}
    connection = Couchbase.new(mock.connstr)
    assert_equal :document, connection.default_format
    refute connection.set(uniq_id, orig_doc).error
    res = connection.get(uniq_id)
    assert_instance_of Hash, res.value
    assert_equal 'Twoflower', res.value['name']
    assert_equal 'The tourist', res.value['role']
  end

  def test_it_raises_error_for_document_format_when_neither_to_json_nor_to_s_defined
    assert_match(/Yajl$/, MultiJson.engine.name)
    orig_doc = SkinyClass.new("Twoflower", "The tourist")
    refute orig_doc.respond_to?(:to_s)
    refute orig_doc.respond_to?(:to_json)

    connection = Couchbase.new(mock.connstr, :default_format => :document)
    assert_raises(Couchbase::Error::ValueFormat) do
      connection.set(uniq_id, orig_doc)
    end

    class << orig_doc
      def to_json
        MultiJson.dump(:name => name, :role => role)
      end
    end
    refute connection.set(uniq_id, orig_doc).error

    class << orig_doc
      undef to_json
      def to_s
        MultiJson.dump(:name => name, :role => role)
      end
    end
    refute connection.set(uniq_id, orig_doc).error
  end

  def test_it_could_dump_arbitrary_class_using_marshal_format
    orig_doc = ArbitraryClass.new("Twoflower", "The tourist")
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, orig_doc, :format => :marshal).error
    res = connection.get(uniq_id)
    refute res.error
    assert_instance_of ArbitraryClass, res.value
    assert_equal 'Twoflower', res.value.name
    assert_equal 'The tourist', res.value.role
  end

  def test_it_accepts_only_string_in_plain_mode
    connection = Couchbase.new(mock.connstr, :default_format => :plain)
    refute connection.set(uniq_id, "1").error

    assert_raises(Couchbase::Error::ValueFormat) do
      connection.set(uniq_id, 1)
    end

    assert_raises(Couchbase::Error::ValueFormat) do
      connection.set(uniq_id, :foo => "bar")
    end
  end

  def test_bignum_conversion
    connection = Couchbase.new(mock.connstr, :default_format => :plain)
    cas = 0xffff_ffff_ffff_ffff
    assert_instance_of Integer, cas
    res = connection.delete(uniq_id, cas)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
  end

  def test_it_allows_to_turn_off_transcoder
    connection = Couchbase.new(mock.connstr, :transcoder => nil)
    refute connection.set(uniq_id, '{"foo":   42}').error
    res = connection.get(uniq_id)
    refute res.error
    assert_equal '{"foo":   42}', res.value
  end

  require 'zlib'
  # This class wraps any other transcoder and performs compression
  # using zlib
  class ZlibTranscoder
    FMT_ZLIB = 0x04

    def initialize(base)
      @base = base
    end

    def dump(obj, flags, options = {})
      obj, flags = @base.dump(obj, flags, options)
      z = Zlib::Deflate.new(Zlib::BEST_SPEED)
      buffer = z.deflate(obj, Zlib::FINISH)
      z.close
      [buffer, flags | FMT_ZLIB]
    end

    def load(blob, flags, options = {})
      # decompress value only if Zlib flag set
      if (flags & FMT_ZLIB) == FMT_ZLIB
        z = Zlib::Inflate.new
        blob = z.inflate(blob)
        z.finish
        z.close
      end
      @base.load(blob, flags, options)
    end
  end

  def test_it_can_use_custom_transcoder
    connection = Couchbase.new(mock.connstr)
    connection.transcoder = ZlibTranscoder.new(Couchbase::Transcoder::Document)

    refute connection.set(uniq_id, "foo" => "bar").error

    res = connection.get(uniq_id)
    refute res.error
    assert_equal({"foo" => "bar"}, res.value)

    connection.transcoder = nil
    res = connection.get(uniq_id)
    refute res.error
    assert_equal "x\u0001\xABVJ\xCB\xCFW\xB2RJJ,R\xAA\u0005\u0000\u001Dz\u00044", res.value
  end
end
