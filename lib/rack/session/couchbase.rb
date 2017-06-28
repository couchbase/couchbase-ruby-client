# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2017 Couchbase, Inc.
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

require 'rack/session/abstract/id'
require 'couchbase'
require 'thread'

module Rack
  module Session
    # This is Couchbase-powered session store for rack applications
    #
    # To use it just load it as usual middleware in your `config.ru` file
    #
    #   require 'rack/session/couchbase'
    #   use Rack::Session::Couchbase
    #
    # You can also pass additional options:
    #
    #   require 'rack/session/couchbase'
    #   use Rack::Session::Couchbase, :expire_after => 5.minutes,
    #     :couchbase => {:bucket => "sessions", :default_format => :document}
    #
    # By default sessions will be serialized using Marshal class. But
    # you can store them as JSON (+:default_format => :document+), to
    # allow analyse them using Map/Reduce. In this case you should
    # care about serialization of all custom objects like
    # ActionDispatch::Flash::FlashHash
    #
    class Couchbase < Abstract::ID
      attr_reader :mutex, :pool

      DEFAULT_OPTIONS = Abstract::ID::DEFAULT_OPTIONS.merge(
        :couchbase => {:quiet => true, :default_format => :marshal,
                       :key_prefix => 'rack:session:'}
      )

      def initialize(app, options = {})
        # Support old :expires option
        options[:expire_after] ||= options[:expires]
        super

        @default_options[:couchbase][:default_ttl] ||= options[:expire_after]
        # FIXME: namespace should be implemented in the session store adapter
        @default_options[:couchbase][:key_prefix] ||= options[:namespace]
        @namespace = @default_options[:couchbase][:key_prefix]
        @mutex = Mutex.new
        @pool = ::Couchbase.connect(@default_options[:couchbase])
      end

      def generate_sid
        loop do
          sid = super
          break sid unless @pool.get(sid)
        end
      end

      def get_session(env, sid)
        with_lock(env, [nil, {}]) do
          unless sid && (session = @pool.get(sid))
            sid, session = generate_sid, {}
            @pool.set(sid, session)
          end
          [sid, session]
        end
      end

      def set_session(env, session_id, new_session, options)
        with_lock(env, false) do
          @pool.set(session_id, new_session, options)
          session_id
        end
      end

      def destroy_session(env, session_id, options)
        with_lock(env) do
          @pool.delete(session_id)
          generate_sid unless options[:drop]
        end
      end

      def with_lock(env, default = nil)
        @mutex.lock if env['rack.multithread']
        yield
      rescue ::Couchbase::Error::Connect, ::Couchbase::Error::Timeout
        if $VERBOSE
          warn "#{self} is unable to find Couchbase server."
          warn $ERROR_INFO.inspect
        end
        default
      ensure
        @mutex.unlock if @mutex.locked?
      end
    end
  end
end
