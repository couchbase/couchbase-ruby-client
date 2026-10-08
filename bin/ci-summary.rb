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

# Writes one Markdown summary for a whole workflow run to $GITHUB_STEP_SUMMARY
# and prints an error annotation for each of the first MAX_ANNOTATIONS failing
# tests.
#
#   bin/ci-summary.rb REPORTS_DIR
#
# REPORTS_DIR holds one "junit-<job name>" directory per test job, as
# actions/download-artifact lays them out. Job results and artifacts come from
# the Actions API through `gh`, so GH_TOKEN needs `actions: read`. With
# SUMMARY_CLEANUP=true and `actions: write` it also deletes artifacts that are
# no longer needed:
#
# * per-Ruby binary gems once their fat gem exists;
# * JUnit reports when every job succeeded. After a failure they stay, so that
#   "Re-run failed jobs" can still summarize the jobs it does not re-run.

require "json"
require "open3"
require "rexml/document"

ICONS = {
  "success" => "✅",
  "failure" => "❌",
  "cancelled" => "🚫",
  "skipped" => "⏭️",
  "timed_out" => "⏱️",
}.freeze
GOOD_CONCLUSIONS = %w[success skipped].freeze
BAD_OUTCOMES = [:failure, :error].freeze
MAX_FAILURES = 30
MAX_ANNOTATIONS = 10
MAX_BODY_LINES = 40

TestCase = Struct.new(:id, :file, :line, :time, :outcome, :message, :body)

def gh(*)
  out, err, status = Open3.capture3("gh", *)
  [out, status.success? ? nil : err.strip]
end

def gh_list(path, filter)
  out, err = gh("api", "--paginate", path, "--jq", filter)
  abort("gh api #{path}: #{err}") if err
  out.lines.map { |line| JSON.parse(line) }
end

def human_size(bytes)
  units = %w[B KiB MiB GiB]
  exp = bytes.zero? ? 0 : [(Math.log(bytes) / Math.log(1024)).floor, units.size - 1].min
  format("%<size>.1f %<unit>s", size: bytes.to_f / (1024**exp), unit: units[exp])
end

def human_time(seconds)
  seconds = seconds.round
  seconds < 60 ? "#{seconds}s" : format("%<min>dm %<sec>02ds", min: seconds / 60, sec: seconds % 60)
end

def parse_reports(dir)
  Dir.glob(File.join(dir, "**", "*.xml")).flat_map do |path|
    REXML::Document.new(File.read(path)).get_elements("//testcase").map do |tc|
      problem = tc.elements["failure"] || tc.elements["error"]
      outcome =
        if problem then problem.name.to_sym
        elsif tc.elements["skipped"] then :skipped
        else :passed
        end
      TestCase.new(
        "#{tc.attributes['classname']}##{tc.attributes['name']}",
        tc.attributes["file"],
        tc.attributes["lineno"],
        tc.attributes["time"].to_f,
        outcome,
        problem&.attributes&.[]("message"),
        problem&.text.to_s.strip,
      )
    end
  end
end

def link(text, url)
  url ? "[#{text}](#{url})" : text
end

repo = ENV.fetch("GITHUB_REPOSITORY")
run_id = ENV.fetch("GITHUB_RUN_ID")
attempt = ENV.fetch("GITHUB_RUN_ATTEMPT", "1")
reports_dir = ARGV.fetch(0)

jobs = gh_list("repos/#{repo}/actions/runs/#{run_id}/attempts/#{attempt}/jobs?per_page=100",
               ".jobs[] | {name, status, conclusion, html_url}")
jobs.reject! { |job| job["name"] == ENV.fetch("SUMMARY_JOB_NAME", "summary") }
jobs_by_name = jobs.to_h { |job| [job["name"], job] }
failed_jobs = jobs.reject { |job| GOOD_CONCLUSIONS.include?(job["conclusion"]) }

suites = (Dir.exist?(reports_dir) ? Dir.children(reports_dir) : []).sort.filter_map do |entry|
  name = entry.delete_prefix("junit-")
  next if name == entry

  cases = parse_reports(File.join(reports_dir, entry))
  {name: name, url: jobs_by_name.dig(name, "html_url"), cases: cases}
end

# Cleanup. A read-only token (pull requests from forks) cannot delete; those
# artifacts expire through their retention-days instead.
artifacts = gh_list("repos/#{repo}/actions/runs/#{run_id}/artifacts?per_page=100",
                    ".artifacts[] | select(.expired | not) | {id, name, size_in_bytes}")
names = artifacts.to_set { |artifact| artifact["name"] }
obsolete = artifacts.select do |artifact|
  name = artifact["name"]
  (name.start_with?("couchbase-") && name =~ /-\d+\.\d+\z/ && names.include?(name.sub(/-\d+\.\d+\z/, ""))) ||
    (name.start_with?("junit-") && failed_jobs.empty?)
end
obsolete = [] unless ENV["SUMMARY_CLEANUP"] == "true"
deleted = []
cleanup_errors = []
obsolete.each do |artifact|
  _, err = gh("api", "--method", "DELETE", "repos/#{repo}/actions/artifacts/#{artifact['id']}")
  err ? cleanup_errors << "#{artifact['name']}: #{err}" : deleted << artifact["id"]
end
artifacts.reject! { |artifact| deleted.include?(artifact["id"]) }

out = []
title = ENV.fetch("SUMMARY_TITLE", "Workflow")
out << if failed_jobs.empty?
         "## ✅ #{title}: #{jobs.size} jobs succeeded"
       else
         "## ❌ #{title}: #{failed_jobs.size} of #{jobs.size} jobs did not succeed"
       end
out << ENV["SUMMARY_SUBTITLE"] if ENV["SUMMARY_SUBTITLE"]
out << ""

all_cases = suites.flat_map { |suite| suite[:cases].map { |tc| [suite, tc] } }
unless suites.empty?
  count = lambda { |cases, outcome| cases.count { |tc| tc.outcome == outcome } }
  out << "### Tests"
  out << ""
  out << "| Configuration | | Tests | Passed | Failed | Errors | Skipped | Time |"
  out << "|---|---|--:|--:|--:|--:|--:|--:|"
  rows = suites.map { |suite| [link(suite[:name], suite[:url]), suite[:cases]] }
  rows << ["**Total**", all_cases.map(&:last)]
  rows.each do |label, cases|
    bad = count.call(cases, :failure) + count.call(cases, :error)
    icon = if cases.empty? then "⚠️"
           elsif bad.zero? then "✅"
           else "❌"
           end
    cells = [cases.size, count.call(cases, :passed), count.call(cases, :failure), count.call(cases, :error),
             count.call(cases, :skipped), human_time(cases.sum(&:time))]
    out << "| #{label} | #{icon} | #{cells.join(' | ')} |"
  end
  out << ""
  out << "⚠️ marks a job that produced a report without test cases." if rows.any? { |_, cases| cases.empty? }
  out << ""
end

failures = all_cases.select { |_, tc| BAD_OUTCOMES.include?(tc.outcome) }.group_by { |_, tc| tc.id }
unless failures.empty?
  out << "### Failed tests"
  out << ""
  out << "| Test | Configurations |"
  out << "|---|---|"
  ranked = failures.sort_by { |id, hits| [-hits.size, id] }
  ranked.each do |id, hits|
    out << "| `#{id}` | #{hits.map { |suite, _| link(suite[:name], suite[:url]) }.join(', ')} |"
  end
  out << ""
  ranked.first(MAX_FAILURES).each do |id, hits|
    out << "<details><summary><code>#{id}</code> (#{hits.size})</summary>"
    out << ""
    hits.each do |suite, tc|
      body = tc.body.lines
      body = body.first(MAX_BODY_LINES) + ["... #{body.size - MAX_BODY_LINES} more lines\n"] if body.size > MAX_BODY_LINES
      out << [link(suite[:name], suite[:url]), tc.file && "`#{tc.file}:#{tc.line}`"].compact.join(", ")
      out << ""
      out << "```"
      out << body.join.rstrip
      out << "```"
    end
    out << ""
    out << "</details>"
  end
  out << "" << "#{ranked.size - MAX_FAILURES} more failed tests are listed in the table only." if ranked.size > MAX_FAILURES
  out << ""

  ranked.first(MAX_ANNOTATIONS).each do |id, hits|
    tc = hits.first.last
    message = "Failed in #{hits.map { |suite, _| suite[:name] }.join(', ')}\n\n#{tc.message}"
    location = tc.file ? "file=#{tc.file},line=#{tc.line}," : ""
    puts "::error #{location}title=#{id}::#{message.gsub('%', '%25').gsub("\r", '%0D').gsub("\n", '%0A')}"
  end
end

unless all_cases.empty?
  out << "<details><summary>Slowest tests</summary>"
  out << ""
  out << "| Test | Configuration | Time |"
  out << "|---|---|--:|"
  all_cases.max_by(15) { |_, tc| tc.time }.each do |suite, tc|
    out << "| `#{tc.id}` | #{link(suite[:name], suite[:url])} | #{human_time(tc.time)} |"
  end
  out << ""
  out << "</details>"
  out << ""
end

out << "### Jobs"
out << ""
out << "| Job | Result |"
out << "|---|---|"
jobs.group_by { |job| job["name"][/\A[^ ]+/] }.each do |base, group|
  cells = group.sort_by { |job| job["name"] }.map do |job|
    variant = job["name"][/\((.*)\)\z/, 1]
    icon = ICONS.fetch(job["conclusion"].to_s, "⏳")
    link([icon, variant].compact.join(" "), job["html_url"])
  end
  out << "| `#{base}` | #{cells.join(' ')} |"
end
out << ""

out << "### Artifacts"
out << ""
out << "| Artifact | Size |"
out << "|---|--:|"
artifacts.sort_by { |artifact| artifact["name"] }.each do |artifact|
  url = "https://github.com/#{repo}/actions/runs/#{run_id}/artifacts/#{artifact['id']}"
  out << "| #{link(artifact['name'], url)} | #{human_size(artifact['size_in_bytes'])} |"
end
unless cleanup_errors.empty?
  out << ""
  out << "#{cleanup_errors.size} intermediate artifacts were not deleted and expire with their retention period. " \
         "First error: #{cleanup_errors.first.lines.first.strip}"
end
out << ""

File.write(ENV.fetch("GITHUB_STEP_SUMMARY", "/dev/stdout"), out.join("\n"), mode: "a")
