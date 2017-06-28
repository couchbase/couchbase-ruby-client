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

desc 'Run benchmarks and compare them to memcached and dalli gems'
task :benchmark => [:clean, :compile] do
  cd File.expand_path(File.join(__FILE__, '..', '..', 'test', 'profile')) do
    sh "bundle install && bundle exec ruby benchmark.rb | tee benchmark-#{RUBY_VERSION}p#{RUBY_PATCHLEVEL}.log"
  end
end
