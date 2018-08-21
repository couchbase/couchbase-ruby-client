# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2018 Couchbase, Inc.
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

require File.join(__dir__, 'setup')

class TestViews < MiniTest::Test
  def mock_params
    {:load_beer_sample => true}
  end

  def test_view_trivial
    connection = Couchbase.new("#{mock.connstr}/beer-sample")
    ddocs = connection.design_docs
    assert_equal 1, ddocs.size

    beer = ddocs['beer']
    assert_instance_of Couchbase::DesignDoc, beer
    assert_respond_to beer, :brewery_beers

    view = beer.brewery_beers
    assert_kind_of Enumerable, view
    rows = view.fetch(:limit => 10)
    assert_equal 10, rows.size
    assert_equal 7303, rows.total_rows

    doc = rows.first
    assert_equal '21st_amendment_brewery_cafe', doc.id
    assert_equal ['21st_amendment_brewery_cafe'], doc.key
    assert_nil doc.value
    assert_nil doc.doc
    assert_nil doc.meta

    rows = beer.brewery_beers.fetch(:limit => 10, :include_docs => true)
    assert_equal 10, rows.size
    assert_equal 7303, rows.total_rows

    doc = rows.first
    assert_equal '21st_amendment_brewery_cafe', doc.id
    assert_equal ['21st_amendment_brewery_cafe'], doc.key
    assert_nil doc.value
    assert_instance_of Hash, doc.doc
    assert_equal 'brewery', doc.doc['type']
    assert_equal '21st Amendment Brewery Cafe', doc.doc['name']
    assert_instance_of Hash, doc.meta
    assert_kind_of Numeric, doc.meta[:cas]
  end
end
