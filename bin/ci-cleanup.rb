#!/usr/bin/env ruby
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

# Deletes the intermediate artifacts of a completed run of the tests workflow:
#
# * per-Ruby binary gems whose fat gem exists;
# * JUnit reports, when the run succeeded. After a failure they stay, so that
#   "Re-run failed jobs" can still summarize the jobs it does not re-run.
#
#   bin/ci-cleanup.rb RUN_ID
#
# tests-cleanup.yml runs this from the default branch with an `actions: write`
# token in GH_TOKEN. The run being cleaned up may come from any pull request,
# including one from a fork, so its artifact names are untrusted: they are only
# compared, and printed in quoted form. Artifacts are deleted by numeric id.
#
# bin/ci-summary.rb loads this file for CiCleanup.intermediate, so that the
# summary leaves out the per-Ruby gems deleted here.

require "json"
require "open3"

module CiCleanup
  WORKFLOW_PATH = ".github/workflows/tests.yml"
  # "couchbase-<version>-<platform>-<ruby>" as the build jobs of tests.yml name
  # them; captures the name of the fat gem built from it.
  PER_RUBY_GEM = /\A(couchbase-[^-]+-(?:x86_64|aarch64|arm64|x64)-(?:linux-musl|linux|darwin|mingw))-\d+\.\d+\z/

  module_function

  # Every value that becomes part of an API path or a URL goes through one of
  # these, so a malformed one stops the script instead of reaching `gh api`.
  def repo!(value)
    raise ArgumentError, "repository must be OWNER/NAME, got #{value.inspect}" unless
      value.is_a?(String) && value.match?(%r{\A[A-Za-z0-9][A-Za-z0-9-]*/(?!\.+\z)[A-Za-z0-9._-]+\z}) # rubocop:disable Style/RegexpLiteral

    value
  end

  def id!(value)
    raise ArgumentError, "id must be a positive integer, got #{value.inspect}" unless
      (value.is_a?(Integer) && value.positive?) || (value.is_a?(String) && value.match?(/\A[1-9]\d*\z/))

    value.to_s
  end

  # Artifacts of one run that are no longer needed: [{"id", "name"}, ...].
  def intermediate(artifacts, run_succeeded:)
    names = artifacts.map { |artifact| artifact["name"] }
    artifacts.select do |artifact|
      fat = artifact["name"][PER_RUBY_GEM, 1]
      (fat && names.include?(fat)) || (run_succeeded && artifact["name"].start_with?("junit-"))
    end
  end

  def gh(*args, allow_not_found: false)
    out, err, status = Open3.capture3("gh", *args)
    return out if status.success? || (allow_not_found && err.include?("HTTP 404"))

    raise "gh #{args.first(3).join(' ')}: #{err.strip.lines.first&.strip}"
  end

  def run(repo, run_id)
    repo = repo!(repo)
    run_id = id!(run_id)
    run = JSON.parse(gh("api", "repos/#{repo}/actions/runs/#{run_id}"))
    # Otherwise a run id typed into workflow_dispatch could name a run of another workflow.
    raise "run #{run_id} belongs to #{run['path'].inspect}, not #{WORKFLOW_PATH}" unless run["path"] == WORKFLOW_PATH
    raise "run #{run_id} is #{run['status'].inspect}, not completed" unless run["status"] == "completed"

    artifacts = gh("api", "--paginate", "repos/#{repo}/actions/runs/#{run_id}/artifacts?per_page=100",
                   "--jq", ".artifacts[] | select(.expired | not) | {id, name}").lines.map { |line| JSON.parse(line) }
    intermediate(artifacts, run_succeeded: run["conclusion"] == "success").each do |artifact|
      # inspect quotes the name, so no line printed here can start a workflow command.
      puts "deleting #{artifact['name'].inspect}"
      # 404: another clean-up of the same run deleted it first.
      gh("api", "--method", "DELETE", "repos/#{repo}/actions/artifacts/#{id!(artifact['id'])}", allow_not_found: true)
    end
  end
end

CiCleanup.run(ENV.fetch("GITHUB_REPOSITORY"), ARGV.fetch(0)) if $PROGRAM_NAME == __FILE__
