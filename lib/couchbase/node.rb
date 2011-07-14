# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011 Couchbase, Inc.
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
  class Node
    attr_accessor :status, :hostname, :ports, :couch_api_base

    def initialize(status, hostname, ports, couch_api_base)
      @status = status
      @hostname = hostname
      @ports = ports
      @couch_api_base = couch_api_base
    end

    %w(healthy warmup unhealthy).each do |status|
      class_eval(<<-EOM)
        def #{status}?
          @status == '#{status}'
        end
      EOM
    end

    # # temporary remapping to standalone couchdb instance
    # def couch_api_base
    #   "http://localhost:5995/posts"
    # end
  end
end
