#    Copyright 2020 Couchbase, Inc.
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

require 'couchbase/collection'

module Couchbase
  class Scope
    attr_reader :bucket_name
    attr_reader :name

    alias_method :inspect, :to_s

    # @param [Couchbase::Backend] backend
    # @param [String] bucket_name name of the bucket
    # @param [String, :_default] scope_name name of the scope
    def initialize(backend, bucket_name, scope_name)
      @backend = backend
      @bucket_name = bucket_name
      @name = scope_name
    end

    # Opens the default collection for this scope
    #
    # @return [Collection]
    def default_collection
      Collection.new(@backend, @bucket_name, @name, :_default)
    end

    # Opens the default collection for this scope
    #
    # @param [String] collection_name name of the collection
    #
    # @return [Collection]
    def collection(collection_name)
      Collection.new(@backend, @bucket_name, @name, collection_name)
    end
  end
end
