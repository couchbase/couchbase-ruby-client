# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2013-2017 Couchbase, Inc.
# License:: Apache License, Version 2.0

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

require 'active_support/cache'
require 'action_dispatch/middleware/session/abstract_store'
require 'rack/session/couchbase'
require 'couchbase'

module ActionDispatch
  module Session
    # This is Couchbase-powered session store for Rails applications
    #
    # To use it just update your `config/initializers/session_store.rb` file
    #
    #   require 'action_dispatch/middleware/session/couchbase_store'
    #   AppName::Application.config.session_store :couchbase_store
    #
    # Or remove this file and add following line to your `config/application.rb`:
    #
    #   require 'action_dispatch/middleware/session/couchbase_store'
    #   config.session_store :couchbase_store
    #
    # You can also pass additional options:
    #
    #   require 'action_dispatch/middleware/session/couchbase_store'
    #   session_options = {
    #     :expire_after => 5.minutes,
    #     :couchbase => {:bucket => "sessions", :default_format => :marshal}
    #   }
    #   config.session_store :couchbase_store, session_options
    #
    # By default sessions will be serialized to JSON, to allow analyse them
    # using Map/Reduce.
    #
    class CouchbaseStore < Rack::Session::Couchbase
      include Compatibility
      include StaleSessionCheck
    end
  end
end
