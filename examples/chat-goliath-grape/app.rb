# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2013 Couchbase, Inc.
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

require 'em-synchrony'
require 'couchbase'
require 'goliath'
require 'grape'
require 'date'

class Chat < Grape::API

  format :json

  resource 'messages' do
    get do
      view = env.couchbase.design_docs["messages"].all(:include_docs => true)
      msgs = view.map do |r|
        {
          "id" => r.id,
          "key" => r.key,
          "value" => r.value,
          "cas" => r.meta["cas"],
          # "doc" => r.doc
        }
      end
      {"ok" => true, "messages" => msgs}
    end

    post do
      payload = {
        "timestamp" => DateTime.now.iso8601,
        "message" => params["message"]
      }
      id = env.couchbase.incr("msgid", :initial => 1)
      id = "msg:#{id}"
      cas = env.couchbase.set(id, payload)
      {"ok" => true, "id" => id, "cas" => cas}
    end
  end

end

class App < Goliath::API
  def response(env)
    Chat.call(env)
  rescue => e
    [
      500,
      {'Content-Type' => 'application/json'},
      MultiJson.dump(:error => e, :stacktrace => e.backtrace)
    ]
  end
end
