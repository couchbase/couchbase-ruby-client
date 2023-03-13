# frozen_string_literal: true

module Couchbase
  module Protostellar
    class RequestBehaviour
      attr_reader :retry_duration
      attr_reader :error

      def initialize(retry_duration: nil, error: nil)
        @retry_duration = retry_duration
        @error = error
      end

      def self.fail(error)
        RequestBehaviour.new(error: error)
      end

      def self.retry(retry_duration)
        RequestBehaviour.new(retry_duration: retry_duration)
      end
    end
  end
end
