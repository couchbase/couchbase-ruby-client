# frozen_string_literal: true

#  Copyright 2023 Couchbase, Inc.
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
require "couchbase/protostellar/request_generator/kv"
require "couchbase/protostellar/request_behaviour"
require "couchbase/protostellar/retry/orchestrator"
require "couchbase/protostellar/retry/reason"
require "couchbase/options"

RSpec.describe Couchbase::Protostellar::Retry::Orchestrator do
  describe "#maybe_retry" do
    let(:request_generator) do
      # The requests here do not get executed - just using some placeholder bucket/scope/collection names
      Couchbase::Protostellar::RequestGenerator::KV.new("default", "foo", "bar")
    end

    context "when the retry reason allows non-idempotent retries" do
      subject(:request_behaviour) do
        request = request_generator.replace_request("a_key", sample_content, Couchbase::Options::Replace::DEFAULT)
        described_class.maybe_retry(request, Couchbase::Protostellar::Retry::Reason::KV_LOCKED)
      end

      it "the behaviour is not an error" do
        expect(request_behaviour.error).to be_nil
      end

      it "the behaviour has the correct retry duration" do
        expect(request_behaviour.retry_duration).to be 1
      end
    end

    context "when the request is idempotent" do
      subject(:request_behaviour) do
        request = request_generator.get_request("a_key", Couchbase::Options::Get::DEFAULT)
        described_class.maybe_retry(request, Couchbase::Protostellar::Retry::Reason::UNKNOWN)
      end

      it "the behaviour is not an error" do
        expect(request_behaviour.error).to be_nil
      end

      it "the behaviour has the correct retry duration" do
        expect(request_behaviour.retry_duration).to be 1
      end
    end

    context "when the request is idempotent and there have been 2 prior retries" do
      subject(:request_behaviour) do
        request = request_generator.get_request("a_key", Couchbase::Options::Get::DEFAULT)

        request.add_retry_attempt(Couchbase::Protostellar::Retry::Reason::UNKNOWN)
        request.add_retry_attempt(Couchbase::Protostellar::Retry::Reason::UNKNOWN)

        described_class.maybe_retry(request, Couchbase::Protostellar::Retry::Reason::UNKNOWN)
      end

      it "the behaviour has the correct retry duration according to the default (best effort) strategy" do
        expect(request_behaviour.retry_duration).to be 4
      end
    end

    context "when the request is not idempotent and the retry reason does not allow non-idempotent retries" do
      subject(:request_behaviour) do
        request = request_generator.replace_request("a_key", sample_content, Couchbase::Options::Replace::DEFAULT)
        described_class.maybe_retry(request, Couchbase::Protostellar::Retry::Reason::UNKNOWN)
      end

      it "the retry duration of the behaviour is nil" do
        expect(request_behaviour.retry_duration).to be_nil
      end

      it "the behaviour's error is set" do
        expect(request_behaviour.error).not_to be_nil
      end

      it "the behaviour has a RequestCanceled error" do
        expect(request_behaviour.error.class).to be Couchbase::Error::RequestCanceled
      end
    end
  end
end
