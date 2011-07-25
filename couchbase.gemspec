# -*- encoding: utf-8 -*-
$:.push File.expand_path('../lib', __FILE__)
require 'couchbase/version'

Gem::Specification.new do |s|
  s.name        = 'couchbase-ruby-client'
  s.version     = Couchbase::VERSION
  s.author      = 'Couchbase'
  s.email       = 'info@couchbase.com'
  s.license     = 'ASL-2'
  s.homepage    = 'http://couchbase.org'
  s.summary     = %q{Couchbase ruby driver}
  s.description = %q{Couchbase ruby driver}

  s.rubyforge_project = 'couchbase-ruby-client'

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ['lib']

  s.add_runtime_dependency 'memcached', '~> 1.2.7'
  s.add_runtime_dependency 'yajl-ruby', '~> 0.8.2'
  s.add_runtime_dependency 'curb', '~> 0.7.15'
  s.add_runtime_dependency 'will_paginate', '~> 2.3'

  s.add_development_dependency 'minitest'
end
