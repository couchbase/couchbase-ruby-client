# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011, 2012 Couchbase, Inc.
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

require 'connection_pool'
require 'pry'
module Couchbase
  class ConnectionPool

    def initialize(pool_size = 5, *args)
      @pool = ::ConnectionPool.new(:size => pool_size) { ::Couchbase::Bucket.new(*args) }
    end

    def get(key, options = {})
      @pool.with do |data|
        data.get(key, options)
      end
    end

    def set(key, value, options = {})
      @pool.with do |data|
        data.set(key, value, options)
      end
    end

    def add(key, value, options = {})
      @pool.with do |data|
        data.add(key, value, options)
      end
    end

    def delete(key, options = {})
      @pool.with do |data|
        data.delete(key, options)
      end
    end

    def incr(name, amount, options = {})
      @pool.with do |data|
        data.incr(name, amount, options)
      end
    end

    def decr(name, amount, options = {})
      @pool.with do |data|
        data.decr(name, amount, options)
      end
    end

    def stats(*arg)
      @pool.with do |data|
        data.stats(*arg)
      end
    end
  end
end
