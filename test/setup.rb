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

require 'minitest/autorun'
require 'couchbase'

require 'socket'

class CouchbaseMock
  Monitor = Struct.new(:pid, :client, :socket, :port)

  attr_accessor :host, :port, :buckets_spec, :num_nodes, :num_vbuckets

  def initialize(params = {})
    @host = "127.0.0.1"
    @port = 0
    @num_nodes = 10
    @num_vbuckets = 4096
    @buckets_spec = "default:"  # "default:,protected:secret,cache::memcache"
    params.each do |key, value|
      send("#{key}=", value)
    end
    yield self if block_given?
    if @num_vbuckets < 1 || (@num_vbuckets & (@num_vbuckets - 1) != 0)
      raise ArgumentError, "Number of vbuckets should be a power of two and greater than zero"
    end
  end

  def start
    @monitor = Monitor.new
    @monitor.socket = TCPServer.new(nil, 0)
    @monitor.socket.listen(10)
    _, @monitor.port, _, _ = @monitor.socket.addr
    trap("CLD") do
      puts "CouchbaseMock.jar died unexpectedly during startup"
      exit(1)
    end
    @monitor.pid = fork
    if @monitor.pid.nil?
      rc = exec(command_line("--harakiri-monitor=:#{@monitor.port}"))
    else
      trap("CLD", "SIG_DFL")
      @monitor.client, _ = @monitor.socket.accept
      @port = @monitor.client.recv(100).to_i
    end
  end

  def stop
    @monitor.client.close
    @monitor.socket.close
    Process.kill("TERM", @monitor.pid)
    Process.wait(@monitor.pid)
  end

  def failover_node(index, bucket = "default")
    @monitor.client.send("failover,#{index},#{bucket}", 0)
  end

  def respawn_node(index, bucket = "default")
    @monitor.client.send("respawn,#{index},#{bucket}", 0)
  end

  protected

  def command_line(extra = nil)
    cmd = "java -jar #{File.dirname(__FILE__)}/CouchbaseMock.jar"
    cmd << " --host #{@host}" if @host
    cmd << " --port #{@port}" if @port
    cmd << " --nodes #{@num_nodes}" if @num_nodes
    cmd << " --vbuckets #{@num_vbuckets}" if @num_vbuckets
    cmd << " --buckets #{@buckets_spec}" if @buckets_spec
    cmd << " #{extra}"
    cmd
  end
end

class MiniTest::Unit::TestCase

  def start_mock(params = {})
    mock = CouchbaseMock.new(params)
    mock.start
    mock
  end

  def stop_mock(mock)
    assert(mock)
    mock.stop
  end

  def with_mock(params = {})
    mock = nil
    if block_given?
      mock = start_mock(params)
      yield mock
    end
  ensure
    stop_mock(mock) if mock
  end

  def uniq_id(*suffixes)
    [caller.first[/.*[` ](.*)'/, 1], suffixes].join("_")
  end
end
