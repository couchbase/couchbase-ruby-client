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

gem 'rake-compiler', '>= 0.7.5'
require "rake/extensiontask"

def gemspec
  @clean_gemspec ||= eval(File.read(File.expand_path('../../couchbase.gemspec', __FILE__)))
end

# Setup compile tasks.  Configuration can be passed via ENV.
# Example:
#  rake compile with_libcouchbase_include=/opt/couchbase/include
#               with_libcouchbase_lib=/opt/couchbase/lib
#
# or
#
#  rake compile with_libcouchbase_dir=/opt/couchbase
#
Rake::ExtensionTask.new("couchbase_ext", gemspec) do |ext|
  CLEAN.include "#{ext.lib_dir}/*.#{RbConfig::CONFIG['DLEXT']}"

  ENV.each do |key, val|
    next unless key =~ /\Awith_(\w+)\z/i
    opt = $1.downcase.tr('_', '-')
    if File.directory?(path = File.expand_path(val))
      ext.config_options << "--with-#{opt}=#{path}"
    else
      warn "No such directory: #{opt}: #{path}"
    end
  end
end

require 'rubygems/package_task'
Gem::PackageTask.new(gemspec) do |pkg|
  pkg.need_zip = true
  pkg.need_tar = true
end
