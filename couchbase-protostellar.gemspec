# frozen_string_literal: true

require_relative "lib/couchbase/protostellar/version"

Gem::Specification.new do |spec|
  spec.name = "couchbase-protostellar"
  spec.version = Couchbase::Protostellar::VERSION
  spec.authors = ["Sergey Avseyev"]
  spec.email = ["sergey.avseyev@gmail.com"]

  spec.summary = "WIP. Extension for Couchbase using the Protostellar protocol"
  spec.description = "Work In Progress extension for Couchbase library to connect using Protostellar, a gRPC-based protocol."
  spec.homepage = "https://github.com/couchbaselabs/couchbase-ruby-client-stellar-nebula"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 2.7.0"

  spec.metadata = {
    "homepage_uri" => "https://docs.couchbase.com/ruby-sdk/3.2/hello-world/start-using-sdk.html",
    "bug_tracker_uri" => "https://couchbase.com/issues/browse/RCBC",
    "mailing_list_uri" => "https://forums.couchbase.com/c/ruby-sdk",
    "source_code_uri" => "https://github.com/couchbase/couchbase-ruby-client-protostellar/tree/#{spec.version}",
    "changelog_uri" => "https://github.com/couchbase/couchbase-ruby-client-protostellar/releases/tag/#{spec.version}",
    "documentation_uri" => "https://docs.couchbase.com/sdk-api/couchbase-ruby-client-#{spec.version}/index.html",
    "github_repo" => "ssh://github.com/couchbase/couchbase-ruby-client",
    "rubygems_mfa_required" => "true",
  }

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(/\A(?:(?:bin|test|spec|features)\/|\.(?:git|travis|circleci)|appveyor)/)
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(/\Aexe\//) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "googleapis-common-protos-types", "~> 1.4.0"
  spec.add_dependency "grpc", "~> 1.50"
end
