# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2014 Couchbase, Inc.
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

require 'couchbase/version'
require 'multi_json'

if RUBY_ENGINE == 'jruby'
  require 'java'
  require 'jruby-client-1.0.0-dp1-all.jar'
  require 'com/couchbase/client/jruby/couchbase'
else
  puts <<EOM
You are using #{RUBY_ENGINE} #{RUBY_VERSION}.
This is development preview of Couchbase SDK 2.0, which works with
JRuby so far. MRI support is coming.
EOM
  exit(1)
end
