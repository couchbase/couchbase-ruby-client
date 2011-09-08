require 'bundler/gem_tasks'

require 'rake/testtask'
Rake::TestTask.new do |test|
  test.libs << "test" << "."
  test.ruby_opts << "-rruby-debug" if ENV['DEBUG']
  test.pattern = 'test/test_*.rb'
  test.verbose = true
end

require 'rubygems/package_task'
def gemspec
  @clean_gemspec ||= eval(File.read(File.expand_path('../couchbase.gemspec', __FILE__)))
end

Gem::PackageTask.new(gemspec) do |pkg|
  pkg.need_zip = true
  pkg.need_tar = true
end

desc 'Start an irb session and load the library.'
task :console do
  exec "irb -I lib -rcouchbase"
end

task :default => :test

