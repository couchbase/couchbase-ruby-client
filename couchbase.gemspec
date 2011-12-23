# -*- encoding: utf-8 -*-
$:.push File.expand_path('../lib', __FILE__)
require 'couchbase/version'

Gem::Specification.new do |s|
  s.name        = 'couchbase'
  s.version     = Couchbase::VERSION
  s.author      = 'Couchbase'
  s.email       = 'info@couchbase.com'
  s.license     = 'ASL-2'
  s.homepage    = 'http://couchbase.org'
  s.summary     = %q{Couchbase ruby driver}
  s.description = %q{The official client library for use with Couchbase Server.}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ['lib']

  s.add_development_dependency 'rake', '0.8.7'
  s.add_development_dependency 'minitest'
  s.add_development_dependency 'mocha'
end
