# frozen_string_literal: true

require "test_helper"

return

require "couchbase/protostellar/request_generator/kv"
require "couchbase/protostellar/request_behaviour"
require "couchbase/protostellar/retry/orchestrator"
require "couchbase/protostellar/retry/reason"
require "couchbase/options"

module Couchbase
  module Protostellar
    class RetryOrchestratorTest < Minitest::Test
      def setup
        # The requests here do not get executed - just using some placeholder bucket/scope/collection names
        @request_generator = RequestGenerator::KV.new("default", "foo", "bar")
      end

      def test_maybe_retry_reason_allows_non_idempotent_retries
        req = @request_generator.replace_request("a_key", {"foo" => "bar"}, Couchbase::Options::Replace::DEFAULT)
        behaviour = Retry::Orchestrator.maybe_retry(req, Retry::Reason::KV_LOCKED)

        assert_nil behaviour.error
        assert_equal 1, behaviour.retry_duration
      end

      def test_maybe_retry_idempotent_request
        req = @request_generator.get_request("a_key", Couchbase::Options::Get::DEFAULT)
        behaviour = Retry::Orchestrator.maybe_retry(req, Retry::Reason::UNKNOWN)

        assert_nil behaviour.error
        assert_equal 1, behaviour.retry_duration
      end

      def test_maybe_retry_idempotent_request_with_two_prior_retries
        req = @request_generator.get_request("a_key", Couchbase::Options::Get::DEFAULT)
        req.add_retry_attempt(Retry::Reason::UNKNOWN)
        req.add_retry_attempt(Retry::Reason::UNKNOWN)

        behaviour = Retry::Orchestrator.maybe_retry(req, Retry::Reason::UNKNOWN)

        assert_nil behaviour.error
        assert_equal 4, behaviour.retry_duration
      end

      def test_maybe_retry_non_idempotent_request_not_allowed
        req = @request_generator.replace_request("a_key", {"foo" => "bar"}, Couchbase::Options::Replace::DEFAULT)
        behaviour = Retry::Orchestrator.maybe_retry(req, Retry::Reason::UNKNOWN)

        assert_nil behaviour.retry_duration
        assert behaviour.error
        assert_instance_of Couchbase::Error::RequestCanceled, behaviour.error
      end
    end
  end
end
