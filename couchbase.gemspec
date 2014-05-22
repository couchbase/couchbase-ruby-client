# -*- encoding: utf-8 -*-
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

$:.push File.expand_path('../lib', __FILE__)
require 'couchbase/version'

Gem::Specification.new do |s|
  s.name        = 'couchbase'
  s.version     = Couchbase::VERSION
  s.author      = 'Couchbase'
  s.email       = 'support@couchbase.com'
  s.license     = 'ASL-2'
  s.homepage    = 'http://couchbase.org'
  s.summary     = %q{Couchbase ruby driver}
  s.description = %q{The official client library for use with Couchbase Server.}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.extensions    = `git ls-files -- ext/**/extconf.rb`.split("\n")
  s.require_paths = ['lib']

  s.add_runtime_dependency 'yaji', '~> 0.3', '>= 0.3.2'
  s.add_runtime_dependency 'multi_json', '~> 1.0'
  s.add_runtime_dependency 'connection_pool', '<= 3.0.0', '>= 1.0.0'

  s.add_development_dependency 'rake'
  s.add_development_dependency 'minitest', '~> 5.0', '>= 5.0.4'
  s.add_development_dependency 'rake-compiler', '~> 0.7', '>= 0.7.5'
  s.add_development_dependency 'mini_portile', '~> 0.5', '>= 0.5.2'
  s.add_development_dependency 'yajl-ruby', '~> 1.1', '>= 1.1.0'
  s.add_development_dependency 'activesupport'
  s.add_development_dependency 'eventmachine'
  s.add_development_dependency 'em-synchrony'
end
