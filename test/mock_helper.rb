#!/usr/bin/env ruby

#  Copyright 2020-2021 Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

require "net/http"
require "json"
require "fileutils"
require "tmpdir"
require "socket"

class Caves
  attr_accessor :verbose

  VERSION = "v0.0.1-72".freeze
  FORK = "couchbaselabs".freeze

  def download_mock(url = caves_url)
    return if binary_ready?

    puts "download #{url}"
    resp = Net::HTTP.get_response(URI.parse(url))

    case resp
    when Net::HTTPSuccess
      raise "Unexpected content type: #{resp['content-type']}" if resp["content-type"] != "application/octet-stream"

      FileUtils.mkdir_p(caves_dir, verbose: verbose?)
      File.write(mock_path, resp.body)
      FileUtils.chmod("a+x", mock_path, verbose: verbose?)
    when Net::HTTPRedirection
      download_mock(resp["location"])
    else
      raise "Unable to download mock from #{url}: #{resp.status}"
    end
  end

  def start
    download_mock unless binary_ready?

    @logs_prefix = Time.now.to_f
    FileUtils.mkdir_p(logs_dir, verbose: verbose?)
    started = false
    until started
      trap("CLD") do
        puts "CAVES died unexpectedly during startup, check logs at #{logs_path}. Retrying"
        next
      end
      @pid = Process.spawn(mock_path, "--control-port=#{control_port}",
                           chdir: caves_dir,
                           out: File.join(logs_dir, "#{@logs_prefix}.out.txt"),
                           err: File.join(logs_dir, "#{@logs_prefix}.err.txt"))
      trap("CLD", "SIG_DFL")
      started = true
    end
    @caves, = @control_sock.accept
    _, caves_port, = @caves.addr
    puts "run #{mock_path}, control_port=#{control_port}, pid=#{@pid}, caves_port=#{caves_port}, logs=#{logs_path}" if verbose?
    hello_cmd = read_command
    raise "CAVES didn't greet us, something happened, check logs at #{logs_path}" if hello_cmd["type"] != "hello"
  end

  # @param [String] cluster_id
  # @return [String] connection string
  def create_cluster(cluster_id)
    resp = round_trip_command("type" => "createcluster", "id" => cluster_id)
    resp["connstr"]
  end

  # @param [String] run_id
  # @param [String] client_name
  # @return [String] connection string
  def start_testing(run_id, client_name)
    resp = round_trip_command("type" => "starttesting", "run" => run_id, "client" => client_name)
    resp["connstr"]
  end

  # @param [String] run_id
  # @return [Hash] report
  def end_testing(run_id)
    resp = round_trip_command("type" => "endtesting", "run" => run_id)
    resp["report"]
  end

  class TestStartedSpec
    attr_accessor :connection_string, :bucket, :scope, :collection

    def initialize
      yield self
    end
  end

  # @param [String] run_id
  # @param [String] test_name
  # @return [TestStartedSpec]
  def start_test(run_id, test_name)
    resp = round_trip_command("type" => "starttest", "run" => run_id, "test" => test_name)
    TestStartedSpec.new do |res|
      res.connection_string = resp["connstr"]
      res.bucket = resp["bucket"]
      res.scope = resp["scope"]
      res.collection = resp["collection"]
    end
  end

  # @param [String] run_id
  # @return [void]
  def end_test(run_id)
    round_trip_command("type" => "endtest", "run" => run_id)
  end

  # @param [String] run_id
  # @param [Integer] amount_ms duration in milliseconds
  # @return [void]
  def time_travel_run(run_id, amount_ms)
    round_trip_command("type" => "timetravel", "run" => run_id, "amount_ms" => amount_ms)
  end

  # @param [String] cluster_id
  # @param [Integer] amount_ms duration in milliseconds
  # @return [void]
  def time_travel_cluster(cluster_id, amount_ms)
    round_trip_command("type" => "timetravel", "cluster" => cluster_id, "amount_ms" => amount_ms)
  end

  def dump_logs
    stderr_log = File.join(logs_dir, "#{@logs_prefix}.err.txt")
    puts File.read(stderr_log) if File.exist?(stderr_log)
  end

  private

  def round_trip_command(cmd)
    warn "CAVES << #{cmd.inspect}" if verbose?
    write_command(cmd)
    resp = read_command
    warn "CAVES >> #{resp.inspect}" if verbose?
    resp
  end

  def write_command(cmd)
    @caves.write("#{JSON.generate(cmd)}\u0000")
  end

  def read_command
    raise "CAVES is not connected" unless defined? @caves

    JSON.parse(@caves.readline("\0", chomp: true))
  end

  def caves_dir
    @caves_dir ||= ENV.fetch("CB_CAVES_DIR", nil) || File.join(Dir.tmpdir, "cb-gocaves")
  end

  def logs_dir
    @logs_dir ||= ENV.fetch("CB_CAVES_LOGS_DIR", nil) || File.join(caves_dir, "logs")
  end

  def logs_path
    "#{logs_dir}/#{@logs_prefix}.{err,out}.txt"
  end

  def verbose?
    @verbose = %w[yes true on 1].include?(ENV.fetch("CB_CAVES_VERBOSE", nil)) unless defined? @verbose
    @verbose
  end

  def caves_url
    go_os = case RUBY_PLATFORM
            when /linux/
              "linux-amd64"
            when /darwin/
              "macos"
            else
              raise "Unknown platform: #{RUBY_PLATFORM}. Sorry, but CAVES does not work here"
            end
    "https://github.com/#{FORK}/gocaves/releases/download/#{VERSION}/gocaves-#{go_os}"
  end

  def mock_path
    File.join(caves_dir, "gocaves")
  end

  def binary_ready?
    File.executable?(mock_path) && `#{mock_path} --help > /dev/null 2>&1`
  end

  def open_control_socket
    @control_sock = TCPServer.new(nil, 0)
    @control_sock.listen(10)
    _, @control_port, = @control_sock.addr
  end

  def control_port
    open_control_socket unless defined? @control_sock

    @control_port
  end
end

if __FILE__ == $PROGRAM_NAME
  def info(connection_string)
    ports = connection_string.scan(/:(\d+),?/).flatten
    puts "--- KV ports:          #{ports.join(',')}"
    puts "--- Connection string: #{connection_string}"
    puts "--- Wireshark filter:  couchbase && (#{ports.map { |p| "tcp.port == #{p}" }.join(' || ')})"
  end

  require "securerandom"
  require "irb"

  caves = Caves.new
  caves.verbose = true
  caves.start
  cluster_id = SecureRandom.uuid
  info caves.create_cluster(cluster_id)
  binding.irb # rubocop:disable Lint/Debugger easy REPL for CAVES
end
