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
  ext.cross_compile = true
  ext.cross_platform = [ENV['HOST'] || "i386-mingw32"]
  if ENV['RUBY_CC_VERSION']
    ext.lib_dir = "lib/couchbase"
  end
  ext.cross_compiling do |spec|
    spec.files.delete("lib/couchbase/couchbase_ext.so")
    spec.files.push("lib/couchbase_ext.rb", Dir["lib/couchbase/1.{8,9}/couchbase_ext.so"])
  end

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
  pkg.need_tar = true
end

require 'mini_portile'
require 'rake/extensioncompiler'

class MiniPortile
  alias :initialize_with_default_host :initialize
  def initialize(name, version)
    initialize_with_default_host(name, version)
    @host = ENV['HOST'] || Rake::ExtensionCompiler.mingw_host
  end

  alias :cook_without_checkpoint :cook
  def cook
    checkpoint = "ports/.#{name}-#{version}-#{host}.installed"
    unless File.exist?(checkpoint)
      cook_without_checkpoint
      FileUtils.touch(checkpoint)
    end
  end
end

namespace :ports do
  directory "ports"

  task :libcouchbase => ["ports"] do
    recipe = MiniPortile.new "libcouchbase", "2.0.0beta2"
    recipe.files << "http://packages.couchbase.com/clients/c/libcouchbase-#{recipe.version}.tar.gz"
    recipe.configure_options.push("--disable-debug",
                                  "--disable-dependency-tracking",
                                  "--disable-couchbasemock",
                                  "--disable-cxx",
                                  "--disable-examples",
                                  "--disable-tools")
    recipe.cook
    recipe.activate
  end
end

file "lib/couchbase_ext.rb" do
  File.open("lib/couchbase_ext.rb", 'wb') do |f|
    f.write <<-RUBY
      require "couchbase/\#{RUBY_VERSION.sub(/\\.\\d+$/, '')}/couchbase_ext"
    RUBY
  end
end

task :cross => ["lib/couchbase_ext.rb", "ports:libcouchbase"]

desc "Package gem for windows"
task "package:windows" => :package do
  sh("env RUBY_CC_VERSION=1.8.7 RBENV_VERSION=1.8.7-p370 rbenv exec bundle exec rake cross compile")
  sh("env RUBY_CC_VERSION=1.9.2 RBENV_VERSION=1.9.2-p320 rbenv exec bundle exec rake cross compile")
  sh("env RUBY_CC_VERSION=1.8.7:1.9.2 RBENV_VERSION=1.9.2-p320 rbenv exec bundle exec rake cross native gem")
end
