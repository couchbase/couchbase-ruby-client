require 'bundler/gem_tasks'

require 'rake/testtask'
Rake::TestTask.new do |test|
  test.pattern = 'test/test_*.rb'
  test.verbose = true
end

desc 'Start an irb session and load the library.'
task :console do
  exec "irb -I lib -rruby-debug -rcouchbase"
end

task :default => :test
