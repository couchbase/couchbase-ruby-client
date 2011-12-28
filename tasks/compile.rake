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
