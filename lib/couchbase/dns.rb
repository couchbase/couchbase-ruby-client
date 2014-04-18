# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2014 Couchbase, Inc.
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

require 'resolv'

module Couchbase
  module DNS
    # Locate bootstrap nodes from a DNS SRV record.
    #
    # @note This is experimental interface. It might change in future
    #       (e.g. service identifiers)
    #
    # The DNS SRV records need to be configured on a reachable DNS server. An
    # example configuration could look like the following:
    #
    #     _cbmcd._tcp.example.com.  0  IN  SRV  20  0  11210 node2.example.com.
    #     _cbmcd._tcp.example.com.  0  IN  SRV  10  0  11210 node1.example.com.
    #     _cbmcd._tcp.example.com.  0  IN  SRV  30  0  11210 node3.example.com.
    #
    #     _cbhttp._tcp.example.com.  0  IN  SRV  20  0  8091 node2.example.com.
    #     _cbhttp._tcp.example.com.  0  IN  SRV  10  0  8091 node1.example.com.
    #     _cbhttp._tcp.example.com.  0  IN  SRV  30  0  8091 node3.example.com.
    #
    # Now if "example.com" is passed in as the argument, the three
    # nodes configured will be parsed and put in the returned URI list. Note that
    # the priority is respected (in this example, node1 will be the first one
    # in the list, followed by node2 and node3). As of now, weighting is not
    # supported.
    #
    # @param name the DNS name where SRV records configured
    # @param bootstrap_protocol (Symbol) the desired protocol for
    #        bootstrapping. See +bootstrap_protocols+ option to
    #        {Couchbase::Bucket#new}. Allowed values +:http, :cccp+
    # @return a list of ordered boostrap URIs by their weight.
    #
    # @example Initialize connection using DNS SRV records
    #
    #    nodes = Couchbase::DNS.locate('example.com', :http)
    #    if nodes.empty?
    #      nodes = ["example.com:8091"]
    #    end
    #    Couchbase.connect(:node_list => nodes)
    def locate(name, bootstrap_protocol = :http)
      service = case bootstrap_protocol
                when :http
                  "_cbhttp"
                when :cccp
                  "_cbmcd"
                else
                  raise ArgumentError, "unknown bootstrap protocol: #{bootstrap_transports}"
                end
      hosts = []
      Resolv::DNS.open do |dns|
        resources = dns.getresources("#{service}._tcp.#{name}", Resolv::DNS::Resource::IN::SRV)
        hosts = resources.sort_by(&:priority).map { |res| "#{res.target}:#{res.port}" }
      end
      hosts
    end

    module_function :locate
  end
end
