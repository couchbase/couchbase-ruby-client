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

def gemspec
  @clean_gemspec ||= eval(File.read(File.expand_path('../../couchbase.gemspec', __FILE__)))
end

require 'rubygems/package_task'
Gem::PackageTask.new(gemspec) do |pkg|
  pkg.need_tar = pkg.need_zip = false
end
