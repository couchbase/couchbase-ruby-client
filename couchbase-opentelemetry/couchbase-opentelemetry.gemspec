# frozen_string_literal: true

#  Copyright 2026-Present Couchbase, Inc.
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
require "couchbase/opentelemetry/version"

Gem::Specification.new do |spec|
  spec.name = "couchbase-opentelemetry"
  spec.version = Couchbase::OpenTelemetry::VERSION
  spec.authors = ["Sergey Avseyev"]
  spec.email = ["sergey.avseyev@gmail.com"]
  spec.summary = "OpenTelemetry integration for the Couchbase Ruby Client"
  spec.description = "OpenTelemetry integration for the Couchbase Ruby Client"
  spec.homepage = "https://www.couchbase.com"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = "> 3.2"

  spec.metadata = {
    "homepage_uri" => "https://docs.couchbase.com/ruby-sdk/current/hello-world/start-using-sdk.html",
    "bug_tracker_uri" => "https://jira.issues.couchbase.com/browse/RCBC",
    "mailing_list_uri" => "https://www.couchbase.com/forums/c/ruby-sdk",
    "source_code_uri" => "https://github.com/couchbase/couchbase-ruby-client/tree/#{spec.version}",
    "changelog_uri" => "https://github.com/couchbase/couchbase-ruby-client/releases/tag/#{spec.version}",
    "documentation_uri" => "https://docs.couchbase.com/sdk-api/couchbase-ruby-client-#{spec.version}/index.html",
    "github_repo" => "https://github.com/couchbase/couchbase-ruby-client",
    "rubygems_mfa_required" => "true",
  }

  spec.files = Dir.glob([
                          "lib/**/*.rb",
                        ], File::FNM_DOTMATCH).select { |path| File.file?(path) }
  spec.bindir = "exe"
  spec.executables = spec.files.grep(/^exe\//) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "concurrent-ruby", "~> 1.3"
  spec.add_dependency "opentelemetry-api", "~> 1.7"
  spec.add_dependency "opentelemetry-metrics-api", "~> 0.4.0"
end
