# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2018 Couchbase, Inc.
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

require 'rake/testtask'
require 'rake/clean'

rule 'test/CouchbaseMock.jar' do |_task|
  jar_version = 'CouchbaseMock-1.5.19.jar'
  jar_path = "https://packages.couchbase.com/clients/c/mock/#{jar_version}"
  sh "wget -O test/CouchbaseMock.jar #{jar_path}"
end

CLOBBER << 'test/CouchbaseMock.jar'

module FileUtils
  alias orig_ruby ruby

  def ruby(*args, &block)
    ruby_interp = File.readlink('/proc/self/exe')
    executable = [ENV['RUBY_PREFIX'], ruby_interp].flatten.compact.join(' ')
    options = Hash === args.last ? args.pop : {}
    if args.length > 1
      sh(*([executable] + args + [options]), &block)
    else
      sh("#{executable} #{args.first}", options, &block)
    end
  end
end

Rake::TestTask.new do |test|
  test.libs << "test" << "."
  test.pattern = 'test/test_*.rb'
  test.options = '--verbose'
end

Rake::Task['test'].prerequisites.unshift('test/CouchbaseMock.jar')

common_flags = %w(
  --log-file=/tmp/valgrind.log
  --tool=memcheck
  --error-limit=no
  --undef-value-errors=no
  --leak-check=full
  --show-reachable=yes
  --num-callers=50
  --track-fds=yes
  --leak-resolution=med
  --max-stackframe=7304328
  --partial-loads-ok=yes
)

desc "Run the test suite under Valgrind memcheck."
task "test:valgrind" do
  ENV['RUBY_PREFIX'] = "valgrind #{common_flags.join(' ')}"
  Rake::Task['test'].invoke
end

desc "Run the test suite under Valgrind memcheck with memory-fill."
task "test:valgrind:mem_fill" do
  local_flags = %w(
    --malloc-fill=6D
    --freelist-vol=100000000
    --free-fill=66
  )
  ENV['RUBY_PREFIX'] = "valgrind #{common_flags.join(' ')} #{local_flags.join(' ')}"
  Rake::Task['test'].invoke
end

desc "Run the test suite under Valgrind memcheck with memory-zero."
task "test:valgrind:mem_zero" do
  local_flags = %w(
    --freelist-vol=100000000
    --malloc-fill=00
    --free-fill=00
  )
  ENV['RUBY_PREFIX'] = "valgrind #{common_flags.join(' ')} #{local_flags.join(' ')}"
  Rake::Task['test'].invoke
end

desc "Run the test suite under Valgrind massif."
task "test:valgrind:massif" do
  local_flags = %w(
    --tool=massif
    --time-unit=B
  )
  ENV['RUBY_PREFIX'] = "valgrind #{local_flags.join(' ')}"
  Rake::Task['test'].invoke
end
