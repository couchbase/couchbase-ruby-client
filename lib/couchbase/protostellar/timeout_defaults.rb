# frozen_string_literal: true

module Couchbase
  module Protostellar
    module TimeoutDefaults
      KEY_VALUE = 2_500
      VIEW = 75_000
      QUERY = 75_000
      ANALYTICS = 75_000
      SEARCH = 75_000
      MANAGEMENT = 75_000
    end
  end
end
