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
require 'rake/extensiontask'

def gemspec
  @clean_gemspec ||= eval(File.read(File.expand_path('../../couchbase.gemspec', __FILE__)))
end

version_router = lambda do |t|
  File.open(t.name, 'wb') do |f|
    f.write <<-RUBY
      require "couchbase/\#{RUBY_VERSION.sub(/\\.\\d+$/, '')}/couchbase_ext"
    RUBY
  end
end

class Platform
  attr_reader :name, :host, :versions

  def initialize(params)
    @name = params[:name]
    @host = params[:host]
    @versions = params[:versions]
  end

  def each_version
    @versions.each do |v|
      yield(v, v[/\d\.\d\.\d/])
    end
  end

  def short_versions
    res = []
    each_version do |_long, short|
      res << short
    end
    res
  end
end

recent = '2.0.0-p353'
CROSS_PLATFORMS = [
  Platform.new(:name => 'x64-mingw32', :host => 'x86_64-w64-mingw32', :versions => %w(1.9.3-p484 2.0.0-p353 2.1.0)),
  Platform.new(:name => 'x86-mingw32', :host => 'i686-w64-mingw32', :versions => %w(1.8.7-p374 1.9.3-p484 2.0.0-p353 2.1.0))
]

# Setup compile tasks.  Configuration can be passed via ENV.
# Example:
#  rake compile with_libcouchbase_include=/opt/couchbase/include
#               with_libcouchbase_lib=/opt/couchbase/lib
#
# or
#
#  rake compile with_libcouchbase_dir=/opt/couchbase
#
Rake::ExtensionTask.new('couchbase_ext', gemspec) do |ext|
  ext.cross_compile = true
  ext.cross_platform = ENV['TARGET']
  if ENV['RUBY_CC_VERSION']
    ext.lib_dir = 'lib/couchbase'
  end
  ext.cross_compiling do |spec|
    spec.files.delete('lib/couchbase/couchbase_ext.so')
    spec.files.push('lib/couchbase_ext.rb', Dir['lib/couchbase/*/couchbase_ext.so'])
    spec.files.push(Dir['lib/couchbase/*/couchbase_ext.so'])
    file "#{ext.tmp_dir}/#{ext.cross_platform}/stage/lib/couchbase_ext.rb", &version_router
  end

  CLEAN.include "#{ext.lib_dir}/*.#{RbConfig::CONFIG['DLEXT']}"

  ENV.each do |key, val|
    next unless key =~ /\Awith_(\w+)\z/i
    opt = Regexp.last_match[1].downcase.tr('_', '-')
    if File.directory?(path = File.expand_path(val))
      ext.config_options << "--with-#{opt}=#{path}"
    else
      warn "No such directory: #{opt}: #{path}"
    end
  end
end

require 'rubygems/package_task'
Gem::PackageTask.new(gemspec) do |pkg|
  pkg.need_tar = pkg.need_zip = false
end

require 'mini_portile'
require 'rake/extensioncompiler'

class MiniPortile
  alias_method :cook_without_checkpoint, :cook
  def cook
    checkpoint = "ports/.#{name}-#{version}-#{host}.installed"
    return if File.exist?(checkpoint)
    cook_without_checkpoint
    FileUtils.touch(checkpoint)
  end

  def configure
    return if configured?

    md5_file = File.join(tmp_path, 'configure.md5')
    digest   = Digest::MD5.hexdigest(computed_options)
    File.open(md5_file, 'w') { |f| f.write digest }

    execute('configure', %(perl cmake/configure #{computed_options}))
  end
end

file 'lib/couchbase_ext.rb', &version_router

desc 'Package gem for windows'
task 'package:windows' => ['package', 'lib/couchbase_ext.rb'] do
  vars = %w(CC CFLAGS CPATH CPP CPPFLAGS LDFLAGS LIBRARY_PATH PATH).each_with_object({}) do |v, h|
    h[v] = ENV[v]
    h
  end
  ENV['LDFLAGS'] = '-static-libgcc'

  CROSS_PLATFORMS.each do |platform|
    ENV['TARGET'] = platform.name
    rm_rf('tmp/ ports/')
    mkdir_p('ports')
    recipe = MiniPortile.new('libcouchbase', '2.4.9')
    recipe.host = platform.host
    recipe.files << "http://packages.couchbase.com/clients/c/libcouchbase-#{recipe.version}.tar.gz"
    recipe.configure_options.push('--disable-cxx',
                                  '--disable-tests',
                                  '--enable-static')
    recipe.cook
    recipe.activate
    platform.each_version do |long, short|
      sh("env RUBY_CC_VERSION=#{short} RBENV_VERSION=#{long} rbenv exec rake cross compile")
    end
    vars.each do |k, v|
      ENV[k] = v
    end
    sh("env RUBY_CC_VERSION=#{platform.short_versions.join(':')} RBENV_VERSION=#{recent} rbenv exec rake cross native gem")
  end
end
