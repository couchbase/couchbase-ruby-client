# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011, 2012 Couchbase, Inc.
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

require File.join(File.dirname(__FILE__), 'setup')

class TestFormat < MiniTest::Unit::TestCase

  ArbitraryClass = Struct.new(:name, :role)
  class SkinyClass < Struct.new(:name, :role)
    undef to_s rescue nil
    undef to_json rescue nil
  end

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_default_document_format
    orig_doc = {'name' => 'Twoflower', 'role' => 'The tourist'}
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    assert_equal :document, connection.default_format
    connection.set(uniq_id, orig_doc)
    doc, flags, cas = connection.get(uniq_id, :extended => true)
    assert_equal 0x00, flags & 0x11
    assert doc.is_a?(Hash)
    assert_equal 'Twoflower', doc['name']
    assert_equal 'The tourist', doc['role']
  end

  def test_it_raises_error_for_document_format_when_neither_to_json_nor_to_s_defined
    if (MultiJson.respond_to?(:engine) ? MultiJson.engine : MultiJson.adapter).name =~ /Yajl$/
      orig_doc = SkinyClass.new("Twoflower", "The tourist")
      refute orig_doc.respond_to?(:to_s)
      refute orig_doc.respond_to?(:to_json)

      connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :default_format => :document)
      assert_raises(Couchbase::Error::ValueFormat) do
        connection.set(uniq_id, orig_doc)
      end

      class << orig_doc
        def to_json
          MultiJson.dump(:name => name, :role => role)
        end
      end
      connection.set(uniq_id, orig_doc) # OK

      class << orig_doc
        undef to_json
        def to_s
          MultiJson.dump(:name => name, :role => role)
        end
      end
      connection.set(uniq_id, orig_doc) # OK
    end
  end

  def test_it_could_dump_arbitrary_class_using_marshal_format
    orig_doc = ArbitraryClass.new("Twoflower", "The tourist")
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    connection.set(uniq_id, orig_doc, :format => :marshal)
    doc, flags, cas = connection.get(uniq_id, :extended => true)
    assert_equal 0x01, flags & 0x11
    assert doc.is_a?(ArbitraryClass)
    assert_equal 'Twoflower', doc.name
    assert_equal 'The tourist', doc.role
  end

  def test_it_accepts_only_string_in_plain_mode
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :default_format => :plain)
    connection.set(uniq_id, "1")

    assert_raises(Couchbase::Error::ValueFormat) do
      connection.set(uniq_id, 1)
    end

    assert_raises(Couchbase::Error::ValueFormat) do
      connection.set(uniq_id, {:foo => "bar"})
    end
  end

end
