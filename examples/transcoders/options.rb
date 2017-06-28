# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2013-2017 Couchbase, Inc.
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

require 'optparse'

# just output extra empty line on CTRL-C
trap("INT") do
  STDERR.puts
  exit
end

OPTIONS = {
  :bucket => "default",
  :hostname => "127.0.0.1",
  :port => 8091,
  :username => nil,
  :password => nil
}

OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options] keys"
  opts.on("-h", "--hostname HOSTNAME", "Hostname to connect to (default: #{OPTIONS[:hostname]}:#{OPTIONS[:port]})") do |v|
    host, port = v.split(':')
    OPTIONS[:hostname] = host.empty? ? '127.0.0.1' : host
    OPTIONS[:port] = port.to_i > 0 ? port.to_i : 8091
  end
  opts.on("-u", "--user USERNAME", "Username to log with (default: none)") do |v|
    OPTIONS[:username] = v
  end
  opts.on("-p", "--password PASSWORD", "Password to log with (default: none)") do |v|
    OPTIONS[:password] = v
  end
  opts.on("-b", "--bucket NAME", "Name of the bucket to connect to (default: #{OPTIONS[:bucket]})") do |v|
    OPTIONS[:bucket] = v
  end
  opts.on_tail("-?", "--help", "Show this message") do
    STDERR.puts opts
    exit
  end
end.parse!
