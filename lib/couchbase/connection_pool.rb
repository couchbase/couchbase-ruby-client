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

module Couchbase
  class ConnectionPool

    def initialize(pool_size = 5, *args)
      @pool = ::ConnectionPool.new(:size => pool_size) { ::Couchbase::Bucket.new(*args) }
    end

    def with
      yield @pool.checkout
    ensure
      @pool.checkin
    end

    def respond_to?(id, *args)
      super || @pool.with { |c| c.respond_to?(id, *args) }
    end

    def method_missing(name, *args, &block)
      define_proxy_method(name)
      send(name, *args, &block)
    end

    protected

    def define_proxy_method(name)
      self.class.class_eval <<-RUBY
        def #{name}(*args, &block)
          @pool.with do |connection|
            connection.send(#{name.inspect}, *args, &block)
          end
        end
      RUBY
    end
  end
end
