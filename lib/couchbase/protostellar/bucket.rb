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

require_relative "scope"

module Couchbase
  module Protostellar
    class Bucket
      attr_reader :name

      def initialize(client, name)
        @client = client
        @name = name
      end

      def scope(name)
        Scope.new(@client, @name, name)
      end

      def collection(name)
        Scope.new(@client, @name, "_default").collection(name)
      end

      def default_collection
        Scope.new(@client, @name, "_default").collection("_default")
      end
    end
  end
end
