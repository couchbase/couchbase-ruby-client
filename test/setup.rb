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

require 'minitest/autorun'
require 'minitest/hooks'

require 'couchbase'

require 'socket'
require 'open-uri'

class CouchbaseServer
  attr_accessor :host, :port, :num_nodes, :buckets_spec

  def real?
    true
  end

  def initialize(params = {})
    @host, @port = ENV['COUCHBASE_SERVER'].split(':')
    @port = @port.to_i

    if @host.nil? || @host.empty? || @port == 0
      raise ArgumentError, "Check COUCHBASE_SERVER variable. It should be hostname:port"
    end

    @config = MultiJson.load(open("http://#{@host}:#{@port}/pools/default"))
    @num_nodes = @config["nodes"].size
    @buckets_spec = params[:buckets_spec] || "default:" # "default:,protected:secret,cache::memcache"
  end

  def start
    # flush all buckets
    return unless ENV['COUCHBASE_FLUSH_BUCKETS']
    @buckets_spec.split(',') do |bucket|
      name, password, = bucket.split(':')
      connection = Couchbase.new(:hostname => @host,
                                 :port => @port,
                                 :username => name,
                                 :bucket => name,
                                 :password => password)
      begin
        connection.flush
      rescue Couchbase::Error::NotSupported
        # on recent server flush is disabled
      end
    end
  end

  def time_travel(offset)
    sleep(offset)
  end

  def stop; end
end

class CouchbaseMock
  class Monitor
    attr_accessor :pid
    attr_accessor :client
    attr_accessor :socket
    attr_accessor :port

    def send_command(spec)
      req = MultiJson.dump(spec)
      client.send("#{req}\n", 0)
      res = MultiJson.load(recv_until("\n"))
      return res['payload'] unless res['error']
      STDERR.puts
      STDERR.puts(res['error'])
      raise "Mock returned error for #{req}"
    end

    def recv_until(term)
      buf = ''
      loop do
        c = client.recv(1)
        return buf if c == term
        buf << c
      end
    end
  end

  attr_accessor :buckets_spec, :num_nodes, :num_vbuckets

  def real?
    false
  end

  def connstr
    "couchbase://#{@host}:#{@port}=http"
  end

  def initialize(params = {})
    @host = "127.0.0.1"
    @port = 0
    @num_nodes = 10
    @num_vbuckets = 4096
    @buckets_spec = "default:" # "default:,protected:secret,cache::memcache"
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
    _, @monitor.port, = @monitor.socket.addr
    trap("CLD") do
      puts "CouchbaseMock.jar died unexpectedly during startup"
      exit(1)
    end
    @monitor.pid = fork
    if @monitor.pid.nil?
      exec(command_line("--harakiri-monitor=:#{@monitor.port}"))
    else
      trap("CLD", "SIG_DFL")
      @monitor.client, = @monitor.socket.accept
      @port = @monitor.recv_until("\0").to_i
    end
  end

  def stop
    @monitor.client.close
    @monitor.socket.close
    Process.kill("TERM", @monitor.pid)
    Process.wait(@monitor.pid)
  end

  def time_travel(offset)
    @monitor.send_command(
      'command' => 'TIME_TRAVEL',
      'payload' => {
        'Offset' => offset
      }
    )
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

class MiniTest::Test
  include Minitest::Hooks

  around :all do |&block|
    @__mock = start_mock
    block.call
    stop_mock(@__mock)
  end

  def mock
    @__mock
  end

  def start_mock(params = {})
    mock = nil
    if ENV['COUCHBASE_SERVER']
      mock = CouchbaseServer.new(params)
      if (params[:port] && mock.port != params[:port]) ||
         (params[:host] && mock.host != params[:host]) ||
         mock.buckets_spec != "default:"
        skip("Unable to configure real cluster. Requested config is: #{params.inspect}")
      end
    else
      mock = CouchbaseMock.new(params)
    end
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
    test_id = [caller.first[/.*[` ](.*)'/, 1], suffixes].compact.join("_")
    @ids ||= {}
    @ids[test_id] ||= Time.now.to_f
    [test_id, @ids[test_id]].join("_")
  end

  def after_teardown
    GC.start
  end
end
