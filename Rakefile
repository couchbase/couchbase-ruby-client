require 'bundler/gem_tasks'

Dir['tasks/*.rake'].sort.each { |f| load f }

task :default => [:clobber, :compile, :test]
