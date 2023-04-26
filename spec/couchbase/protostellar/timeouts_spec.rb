# frozen_string_literal: true

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

require "rspec"

require "couchbase/options"
require "couchbase/protostellar/timeouts"
require "couchbase/protostellar/timeout_defaults"

RSpec.describe Couchbase::Protostellar::Timeouts do
  let(:example_timeout_values) do
    {
      key_value_timeout: 10_000,
      view_timeout: 11_000,
      query_timeout: 12_000,
      analytics_timeout: 13_000,
      search_timeout: 14_000,
      management_timeout: 15_000,
    }
  end

  let(:default_timeout_values) do
    {
      key_value_timeout: Couchbase::Protostellar::TimeoutDefaults::KEY_VALUE,
      view_timeout: Couchbase::Protostellar::TimeoutDefaults::VIEW,
      query_timeout: Couchbase::Protostellar::TimeoutDefaults::QUERY,
      analytics_timeout: Couchbase::Protostellar::TimeoutDefaults::ANALYTICS,
      search_timeout: Couchbase::Protostellar::TimeoutDefaults::SEARCH,
      management_timeout: Couchbase::Protostellar::TimeoutDefaults::MANAGEMENT,
    }
  end

  describe ".from_cluster_options" do
    context "when the timeouts are set in the cluster options" do
      subject(:timeouts) do
        described_class.from_cluster_options(options)
      end

      let(:options) do
        Couchbase::Options::Cluster.new(**example_timeout_values)
      end

      it "returns a Timeouts instance with the timeout values from the cluster options" do
        expect(timeouts).to have_attributes(**example_timeout_values)
      end
    end

    context "when none of the timeouts are set in the cluster options" do
      subject(:timeouts) do
        described_class.from_cluster_options(Couchbase::Options::Cluster.new)
      end

      it "returns a Timeouts instance with the default timeouts" do
        expect(timeouts).to have_attributes(**default_timeout_values)
      end
    end
  end

  describe "#timeout_for_service" do
    subject(:timeouts) do
      described_class.new(**example_timeout_values)
    end

    services = [:analytics, :kv, :query, :search, :view, :bucket_admin, :collection_admin]
    expected_timeouts = [13_000, 10_000, 12_000, 14_000, 11_000, 15_000, 15_000]

    services.zip(expected_timeouts).each do |service, exp_timeout|
      it "returns the correct timeout for the #{service} service" do
        expect(timeouts.timeout_for_service(service)).to be exp_timeout
      end
    end
  end
end
