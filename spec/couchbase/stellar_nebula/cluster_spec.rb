# frozen_string_literal: true

#  Copyright 2022-Present Couchbase, Inc.
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

require "rspec"
require "couchbase/stellar_nebula"

RSpec.describe Couchbase::StellarNebula::Cluster do
  let(:cluster) do
    connect
  end

  describe "#query" do
    context "when performing a simple query" do
      before do
        statement = "SELECT * " \
                    "FROM `travel-sample`.inventory.airline " \
                    "WHERE id > 10000"
        @query_result = cluster.query(statement, Couchbase::Options::Query.new(metrics: true))
        @query_result.transcoder = Couchbase::JsonTranscoder.new
      end

      it "the correct number of rows was returned" do
        expect(@query_result.rows.size).to be 47
      end

      it "the returned number of rows matches the `result_count` field in the metadata" do
        expect(@query_result.meta_data.metrics.result_count).to eq(@query_result.rows.size)
      end
    end
  end
end
