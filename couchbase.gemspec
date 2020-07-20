#    Copyright 2020 Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "couchbase/version"

Gem::Specification.new do |spec|
  spec.name = "couchbase"
  spec.version = Couchbase::VERSION[:sdk]
  spec.authors = ["Sergey Avseyev"]
  spec.email = ["sergey.avseyev@gmail.com"]
  spec.summary = "SDK for Couchbase Server"
  spec.description = "Modern SDK for Couchbase Server"
  spec.homepage = "https://www.couchbase.com"
  spec.license = "Apache-2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/couchbase/couchbase-ruby-client"
  spec.metadata["changelog_uri"] = "#{spec.metadata["source_code_uri"]}/releases"
  spec.metadata["github_repo"] = "ssh://github.com/couchbase/couchbase-ruby-client"

  spec.files = Dir.chdir(File.expand_path("..", __FILE__)) do
    exclude_paths = %w[
        test
        spec
        features
        test_data
        ext/third_party/asio/asio/src/examples
        ext/third_party/asio/asio/src/tests
        ext/third_party/asio/asio/src/tools
        ext/third_party/gsl/tests
        ext/third_party/http_parser/contrib
        ext/third_party/http_parser/fuzzers
        ext/third_party/json/contrib
        ext/third_party/json/doc
        ext/third_party/json/src/example
        ext/third_party/json/src/perf
        ext/third_party/json/src/test
        ext/third_party/json/tests
        ext/third_party/snappy/testdata
        ext/third_party/spdlog/bench
        ext/third_party/spdlog/example
        ext/third_party/spdlog/logos
        ext/third_party/spdlog/scripts
        ext/third_party/spdlog/tests
    ]
    `git ls-files --recurse-submodules -z`
      .split("\x0")
      .reject { |f| f.match(%r{^(#{Regexp.union(exclude_paths)})/}) }
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/extconf.rb"]
  spec.rdoc_options << "--exclude" << "ext/"

  spec.add_development_dependency "bundler", "~> 2.1"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "minitest", "~> 5.14"
  spec.add_development_dependency "minitest-reporters", "~> 1.4"
  spec.add_development_dependency "simplecov-cobertura", "~> 1.3"
end
