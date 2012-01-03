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

module Couchbase
  class Bucket

    # Reads a key's value from the server and yields it to a block. Replaces
    # the key's value with the result of the block as long as the key hasn't
    # been updated in the meantime, otherwise raises
    # Couchbase::Error::KeyExists. CAS stands for "compare and swap", and
    # avoids the need for manual key mutexing. Read more info here:
    #
    #   http://docs.couchbase.org/memcached-api/memcached-api-protocol-text_cas.html
    #
    # @param [String] key
    #
    # @param [Hash] options the options for operation
    # @option options [String] :ttl (self.default_ttl) the time to live of this key
    # @option options [Symbol] :format (self.default_format) format of the value
    # @option options [Fixnum] :flags (self.default_flags) flags for this key
    #
    # @yieldparam [Object, Result] value old value in synchronous mode and
    #   +Result+ object in asynchronous mode.
    # @yieldreturn [Object] new value.
    #
    # @raise [Couchbase::Errors:KeyExists] if the key was updated before the the
    #   code in block has been completed (the CAS value has been changed).
    #
    # @example Implement append to JSON encoded value
    #
    #     c.default_format = :document
    #     c.set("foo", {"bar" => 1})
    #     c.cas("foo") do |val|
    #       val["baz"] = 2
    #       val
    #     end
    #     c.get("foo")      #=> {"bar" => 1, "baz" => 2}
    #
    # @return [Fixnum] the CAS of new value
    def cas(key, options = {})
      options = options.merge(:extended => true)
      if async?
        get(key, options) do |ret|
          val = yield(ret) # get new value from caller
          set(ret.key, val, :cas => ret.cas)
        end
      else
        val, flags, ver = get(key, options)
        val = yield(val) # get new value from caller
        set(key, val, :cas => ver)
      end
    end
    alias :compare_and_swap :cas

  end
end
