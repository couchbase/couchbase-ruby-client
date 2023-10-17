#  Copyright 2020-2021 Couchbase, Inc.
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

lib = File.expand_path("lib", __dir__)
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
  spec.required_ruby_version = "> 3.0"

  spec.metadata = {
    "homepage_uri" => "https://docs.couchbase.com/ruby-sdk/current/hello-world/start-using-sdk.html",
    "bug_tracker_uri" => "https://couchbase.com/issues/browse/RCBC",
    "mailing_list_uri" => "https://forums.couchbase.com/c/ruby-sdk",
    "source_code_uri" => "https://github.com/couchbase/couchbase-ruby-client/tree/#{spec.version}",
    "changelog_uri" => "https://github.com/couchbase/couchbase-ruby-client/releases/tag/#{spec.version}",
    "documentation_uri" => "https://docs.couchbase.com/sdk-api/couchbase-ruby-client-#{spec.version}/index.html",
    "github_repo" => "ssh://github.com/couchbase/couchbase-ruby-client",
    "rubygems_mfa_required" => "true",
  }

  spec.files = Dir.glob([
                          "LICENSE.txt",
                          "README.md",
                          "ext/*.cxx",
                          "ext/*.hxx.in",
                          "ext/*.rb",
                          "ext/CMakeLists.txt",
                          "ext/cache/**/*",
                          "ext/couchbase/CMakeLists.txt",
                          "ext/couchbase/LICENSE.txt",
                          "ext/couchbase/cmake/*",
                          "ext/couchbase/core/**/*",
                          "ext/couchbase/couchbase/**/*",
                          "ext/couchbase/third_party/cxx_function/cxx_function.hpp",
                          "ext/couchbase/third_party/expected/COPYING",
                          "ext/couchbase/third_party/expected/include/**/*",
                          "ext/couchbase/third_party/jsonsl/*",
                          "lib/**/*.rb",
                        ], File::FNM_DOTMATCH).select { |path| File.file?(path) }
  spec.bindir = "exe"
  spec.executables = spec.files.grep(/^exe\//) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/extconf.rb"]
  spec.rdoc_options << "--exclude" << "ext/"

  spec.add_dependency "grpc", "~> 1.59"
end
