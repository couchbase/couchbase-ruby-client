# frozen_string_literal: true

module Couchbase
  # Error types for internal protostellar errors
  module Protostellar
    module Error
      class ProtostellarError < StandardError
      end

      class UnexpectedServiceType < ProtostellarError
      end

      class InvalidRetryBehaviour < ProtostellarError
      end

      class InvalidExpiryType < ProtostellarError
      end
    end
  end
end