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
  spec.required_ruby_version = "> 2.5"

  spec.metadata = {
    "homepage_uri" => "https://docs.couchbase.com/ruby-sdk/3.0/hello-world/start-using-sdk.html",
    "bug_tracker_uri" => "https://couchbase.com/issues/browse/RCBC",
    "mailing_list_uri" => "https://forums.couchbase.com/c/ruby-sdk",
    "source_code_uri" => "https://github.com/couchbase/couchbase-ruby-client/tree/#{spec.version}",
    "changelog_uri" => "https://github.com/couchbase/couchbase-ruby-client/releases/tag/#{spec.version}",
    "documentation_uri" => "https://docs.couchbase.com/sdk-api/couchbase-ruby-client-#{spec.version}/index.html",
    "github_repo" => "ssh://github.com/couchbase/couchbase-ruby-client",
  }

  spec.files = Dir[
    "LICENSE.txt",
    "README.md",
    "ext/*.hxx.in",
    "ext/*.rb",
    "ext/CMakeLists.txt",
    "ext/LICENSE.txt",
    "ext/cmake/*",
    "ext/couchbase/**/*",
    "ext/test/*",
    "ext/third_party/asio/COPYING",
    "ext/third_party/asio/LICENSE*",
    "ext/third_party/asio/asio/include/*.hpp",
    "ext/third_party/asio/asio/include/asio/**/*.[hi]pp",
    "ext/third_party/gsl/CMakeLists.txt",
    "ext/third_party/gsl/LICENSE*",
    "ext/third_party/gsl/ThirdPartyNotices.txt",
    "ext/third_party/gsl/include/**/*",
    "ext/third_party/http_parser/LICENSE*",
    "ext/third_party/http_parser/http_parser.{c,h}",
    "ext/third_party/json/CMakeLists.txt",
    "ext/third_party/json/LICENSE*",
    "ext/third_party/json/include/**/*",
    "ext/third_party/snappy/CMakeLists.txt",
    "ext/third_party/snappy/COPYING",
    "ext/third_party/snappy/cmake/*",
    "ext/third_party/snappy/snappy-c.{h,cc}",
    "ext/third_party/snappy/snappy-internal.h",
    "ext/third_party/snappy/snappy-sinksource.{h,cc}",
    "ext/third_party/snappy/snappy-stubs-internal.{h,cc}",
    "ext/third_party/snappy/snappy-stubs-public.h.in",
    "ext/third_party/snappy/snappy.{h,cc}",
    "ext/third_party/spdlog/CMakeLists.txt",
    "ext/third_party/spdlog/LICENSE",
    "ext/third_party/spdlog/cmake/*",
    "ext/third_party/spdlog/include/**/*",
    "ext/third_party/spdlog/src/**/*",
    "lib/**/*.rb",
  ].select { |path| File.file?(path) }
  spec.bindir = "exe"
  spec.executables = spec.files.grep(/^exe\//) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/extconf.rb"]
  spec.rdoc_options << "--exclude" << "ext/"
end
