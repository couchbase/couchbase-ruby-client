# Useful environment variables:
#
# LOOPS (50000)
#   how many time run exercises
#
# HOST (127.0.0.1)
#   the host where cluster is running. benchmark will use default ports to
#   connect to it (11211 and 8091)
#
# STACK_DEPTH (0)
#   the depth of stack where exercises are run. the benchmark will
#   recursively go to given depth before run
#
# TEST ('')
#   use to run specific test (possible values are: set, get, get-multi,
#   append, prepend, delete, get-missing, append-missing, prepend-missing,
#   set-large, get-large)
#
# CLIENT ('')
#   use to run with specific client (possible values are: couchbase, dalli,
#   memcached, memcached:buffer)
#
# DEBUG ('')
#   show exceptions
#

require "rubygems"
require "bundler/setup"

require 'benchmark'

$LOAD_PATH << File.join(File.dirname(__FILE__), "..", "..", "lib")
require 'couchbase'
require 'memcached'
require 'dalli'

puts `uname -a`
puts File.readlines('/proc/cpuinfo').sort.uniq.grep(/model name|cpu cores/) rescue nil
puts RUBY_DESCRIPTION

class Bench

  def initialize(loops = nil, stack_depth = nil)
    @loops = (loops || 50000).to_i
    @stack_depth = (stack_depth || 0).to_i

    puts "PID is #{Process.pid}"
    puts "Loops is #{@loops}"
    puts "Stack depth is #{@stack_depth}"

    @m_value = Marshal.dump(
      @small_value = ["testing"])
    @m_large_value = Marshal.dump(
      @large_value = [{"test" => "1", "test2" => "2", Object.new => "3", 4 => 4, "test5" => 2**65}] * 2048)

    puts "Small value size is: #{@m_value.size} bytes"
    puts "Large value size is: #{@m_large_value.size} bytes"

    @keys = [
      @k1 = "Short",
      @k2 = "Sym1-2-3::45" * 8,
      @k3 = "Long" * 40,
      @k4 = "Medium" * 8,
      @k5 = "Medium2" * 8,
      @k6 = "Long3" * 40]

    reset_clients

    Benchmark.bm(36) do |x|
      @benchmark = x
    end
  end

  def run(level = @stack_depth)
    level > 0 ? run(level - 1) : run_without_recursion
  end

  private

  def reset_clients
    host = ENV['HOST'] || '127.0.0.1'
    @clients = {
      "dalli" => lambda { Dalli::Client.new("#{host}:11211", :marshal => true, :threadsafe => false) },
      "memcached" => lambda { Memcached::Rails.new("#{host}:11211", :no_block => false, :buffer_requests => false, :binary_protocol => true) },
      "memcached:buffer" => lambda { Memcached::Rails.new("#{host}:11211", :no_block => true, :buffer_requests => true, :binary_protocol => true) },
      "couchbase" => lambda { Couchbase.new("http://#{host}:8091/pools/default/buckets/default", :default_format => :marshal) }
    }
  end

  def benchmark_clients(test_name, populate_keys = true)
    return if ENV["TEST"] and !test_name.include?(ENV["TEST"])

    @clients.keys.each do |client_name|
      next if ENV["CLIENT"] and !client_name.include?(ENV["CLIENT"])

      kid = fork do
        client = @clients[client_name].call
        begin
          if populate_keys
            client.set @k1, @m_value
            client.set @k2, @m_value
            client.set @k3, @m_value
          else
            client.delete @k1
            client.delete @k2
            client.delete @k3
          end

          GC.disable
          @benchmark.report("#{test_name}: #{client_name}") { @loops.times { yield client } }
          STDOUT.flush
        rescue Exception => e
          puts "#{test_name}: #{client_name} => #{e.inspect}" if ENV["DEBUG"]
        end
        exit
      end
      Signal.trap("INT") { Process.kill("KILL", kid); exit }
      Process.wait(kid)
    end
    puts
  end

  def run_without_recursion
    benchmark_clients("set") do |c|
      c.set @k1, @m_value
      c.set @k2, @m_value
      c.set @k3, @m_value
    end

    benchmark_clients("get") do |c|
      c.get @k1
      c.get @k2
      c.get @k3
    end

    benchmark_clients("get_multi") do |c|
      if c.respond_to?(:get_multi)
        c.get_multi @keys
      else
        c.get @keys
      end
    end

    benchmark_clients("append") do |c|
      c.append @k1, @m_value
      c.append @k2, @m_value
      c.append @k3, @m_value
    end

    benchmark_clients("prepend") do |c|
      c.prepend @k1, @m_value
      c.prepend @k2, @m_value
      c.prepend @k3, @m_value
    end

    benchmark_clients("delete") do |c|
      c.delete @k1
      c.delete @k2
      c.delete @k3
    end

    benchmark_clients("get_missing", false) do |c|
      c.get @k1 rescue nil
      c.get @k2 rescue nil
      c.get @k3 rescue nil
    end

    benchmark_clients("append_missing", false) do |c|
      c.append @k1, @m_value rescue nil
      c.append @k2, @m_value rescue nil
      c.append @k3, @m_value rescue nil
    end

    benchmark_clients("prepend_missing", false) do |c|
      c.prepend @k1, @m_value rescue nil
      c.prepend @k2, @m_value rescue nil
      c.prepend @k3, @m_value rescue nil
    end

    benchmark_clients("set_large") do |c|
      c.set @k1, @m_large_value
      c.set @k2, @m_large_value
      c.set @k3, @m_large_value
    end

    benchmark_clients("get_large") do |c|
      c.get @k1
      c.get @k2
      c.get @k3
    end

  end
end

Bench.new(ENV["LOOPS"], ENV["STACK_DEPTH"]).run
