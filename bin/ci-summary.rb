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
# actions/download-artifact lays them out. Job results, job annotations and
# artifacts come from the API through `gh`, so GH_TOKEN needs `actions: read`
# and `checks: read`. With SUMMARY_CLEANUP=true and `actions: write` it also
# deletes artifacts that are no longer needed:
#
# * per-Ruby binary gems once their fat gem exists;
# * JUnit reports when every job succeeded. After a failure they stay, so that
#   "Re-run failed jobs" can still summarize the jobs it does not re-run.
#
# Any input may be missing or broken: each section that cannot be built is
# replaced by a note, the rest of the report is still written, and the exit
# status is 1. "written=true" goes to $GITHUB_OUTPUT once the report is written.

require "cgi"
require "json"
require "open3"
require "rexml/document"
require "time"

ICONS = {
  "success" => "✅",
  "failure" => "❌",
  "cancelled" => "🚫",
  "skipped" => "⏭️",
}.freeze
BAD_OUTCOMES = [:failure, :error].freeze
GOOD_CONCLUSIONS = %w[success skipped].freeze
STOPPED_CONCLUSIONS = %w[failure cancelled].freeze
MAX_FAILURES = 30
MAX_ANNOTATIONS = 10
MAX_BODY_LINES = 40
MAX_BODY_BYTES = 4_000
MAX_FAILED_ROWS = 100
MAX_LINKED_CONFIGURATIONS = 4
MAX_BODIES_PER_TEST = 3
MAX_SUMMARY_BYTES = 1_000_000 # GitHub rejects a step summary over 1 MiB

# Order is the order of the "Failures by cause" table.
CAUSES = {
  test: "🧪 Test failures",
  aborted: "💥 Test run aborted",
  install: "📦 Gem install",
  build: "🔨 Build",
  timeout: "⏱️ Timed out",
  infrastructure: "🏗️ Infrastructure",
  runner: "☁️ Runner lost",
  cancelled: "🛑 Cancelled",
}.freeze

# Step names as tests.yml spells them. Any other failed step is infrastructure.
STEP_KINDS = {
  /\ATest\z/ => :test,
  /\AInstall\z/ => :install,
  /\A(Precompile|Repackage|Build |Generate documentation)/ => :build,
}.freeze
TIMEOUT_PATTERN = /exceeded the maximum execution time|has timed out/
# GitHub's annotations on the jobs of a cancelled run, by a person or by a newer
# run in the same concurrency group.
CANCEL_PATTERN = /\AThe run was canceled by |\ACanceling since a higher priority waiting request/
RUNNER_PATTERN = /lost communication with the server|received a shutdown signal|runner has been (stopped|deprovisioned)/i
GENERIC_PATTERN = /\AProcess completed with exit code|\AThe operation was canceled|No test results found/

TestCase = Struct.new(:id, :file, :line, :time, :outcome, :message, :body)
Failure = Struct.new(:job, :cause, :step, :detail)

GH_STATE = {failed: false} # rubocop:disable Style/MutableConstant
GH_ATTEMPTS = 3

# Returns [stdout, nil] or [nil, error]. Server errors and timeouts are
# retried, because a transient failure would otherwise blank a whole section;
# client errors (HTTP 4xx) are not. Once a call has used up its retries the API
# is treated as down and later calls are not made, so an outage cannot outlast
# the step timeout.
def gh(*args)
  return [nil, "not attempted after an earlier API failure"] if GH_STATE[:failed]

  error = nil
  GH_ATTEMPTS.times do |attempt|
    out, err, status = Open3.capture3("timeout", "60", "gh", *args)
    return [out, nil] if status.success?

    error = err.strip.empty? ? "gh exited with #{status.exitstatus}" : err.strip.lines.first.strip
    return [nil, error] if error.match?(/HTTP 4\d\d/)

    sleep(5 * (2**attempt)) if attempt + 1 < GH_ATTEMPTS
  end
  GH_STATE[:failed] = true
  [nil, error]
end

def gh_list(path, filter)
  out, err = gh("api", "--paginate", path, "--jq", filter)
  raise "GET #{path}: #{err}" if err

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

# Table cell text. Backslashes and pipes are written as entities, so the text
# can neither escape markup nor split the cell. Inside a code span entities are
# not decoded, so code cells replace the characters that would end the cell or
# the span.
def cell(text, limit = 200, code: false)
  text = text.to_s.gsub(/\s+/, " ").strip
  text = "#{text[0, limit]}…" if text.size > limit
  return text.tr("|`", "¦'") if code

  CGI.escapeHTML(text).gsub("\\", "&#92;").gsub("|", "&#124;").tr("`", "'")
end

# Workflow command property values, escaped as @actions/core escapeProperty does.
def property(value)
  value.to_s.gsub("%", "%25").gsub("\r", "%0D").gsub("\n", "%0A").gsub(":", "%3A").gsub(",", "%2C")
end

def link(text, url)
  url ? "[#{text}](#{url})" : text
end

# Returns [test cases, names of files that could not be parsed].
def parse_reports(dir)
  broken = []
  cases = Dir.glob(File.join(dir, "**", "*.xml")).flat_map do |path|
    REXML::Document.new(File.read(path)).get_elements("//testcase").map do |tc|
      problem = tc.elements["failure"] || tc.elements["error"]
      outcome =
        if problem then problem.name.to_sym
        elsif tc.elements["skipped"] then :skipped
        else :passed
        end
      TestCase.new("#{tc.attributes['classname']}##{tc.attributes['name']}", tc.attributes["file"],
                   tc.attributes["lineno"], tc.attributes["time"].to_f, outcome,
                   problem&.attributes&.[]("message"), problem&.text.to_s.strip)
    end
  rescue REXML::ParseException, SystemCallError
    broken << File.basename(path)
    []
  end
  [cases, broken]
end

def describe_cancel(message)
  group = message[/request for (\S+) exists/, 1]
  group ? "Superseded by a newer run in concurrency group #{group}" : message
end

# The first step that failed or was cancelled decides the cause. A failed step
# gives the kind of that step, so a later timeout, for example while collecting
# logs, does not hide the failure before it. A cancelled step, or a job without
# one, takes its reason from the annotations: a cancellation, then a timeout,
# then a lost runner. run_cancel is the cancellation found on any job of the
# run: jobs still queued at that moment carry no annotation of their own.
def classify(job, suite, annotations, run_cancel, stale)
  messages = annotations.map { |annotation| annotation["message"].to_s }
  errors = annotations.select { |annotation| annotation["annotation_level"] == "failure" }.map { |a| a["message"].to_s }
  step = job["steps"].to_a.sort_by { |s| s["number"].to_i }.find { |s| STOPPED_CONCLUSIONS.include?(s["conclusion"]) }
  timeout = messages.find { |message| message.match?(TIMEOUT_PATTERN) }
  cancel = messages.find { |message| message.match?(CANCEL_PATTERN) }
  cancel ||= run_cancel if job["conclusion"] == "cancelled" && timeout.nil?
  runner = messages.find { |message| message.match?(RUNNER_PATTERN) }
  detail = errors.find { |message| message !~ GENERIC_PATTERN }

  cause =
    if step&.[]("conclusion") == "failure"
      if timeout&.include?("'#{step['name']}'") then :timeout
      else STEP_KINDS.find { |pattern, _| step["name"].match?(pattern) }&.last || :infrastructure
      end
    elsif cancel.nil? && timeout then :timeout
    elsif cancel.nil? && runner then :runner
    elsif cancel || step then :cancelled
    else :infrastructure
    end

  if cause == :test
    failed = suite ? suite[:cases].count { |tc| BAD_OUTCOMES.include?(tc.outcome) } : 0
    cause = :aborted unless failed.positive?
    detail = "#{failed} failed #{failed == 1 ? 'test' : 'tests'}" if failed.positive?
    detail ||= stale ? "stale report from an earlier attempt" : "no test report" if suite.nil?
  end
  detail = timeout if cause == :timeout
  detail = runner if cause == :runner
  detail = cancel ? describe_cancel(cancel) : "cancelled; no annotation gives the reason" if cause == :cancelled
  detail ||= "the job did not start" if step.nil? && job["steps"].to_a.empty?
  Failure.new(job, cause, step&.[]("name"), detail)
end

repo = ENV.fetch("GITHUB_REPOSITORY")
run_id = ENV.fetch("GITHUB_RUN_ID")
reports_dir = ARGV.fetch(0)
self_name = ENV.fetch("SUMMARY_JOB_NAME", "summary")
problems = []
section = lambda do |title, &block|
  block.call
rescue StandardError => e
  problems << "#{title}: #{e.class}: #{e.message}"
end

jobs = []
section.call("Jobs") do
  jobs = gh_list("repos/#{repo}/actions/runs/#{run_id}/jobs?filter=latest&per_page=100",
                 ".jobs[] | {id, name, status, conclusion, html_url, started_at, steps: [.steps[]? | {name, number, conclusion}]}")
  jobs.reject! { |job| job["name"] == self_name }
end
jobs_by_name = jobs.to_h { |job| [job["name"], job] }
unsuccessful = jobs.reject { |job| GOOD_CONCLUSIONS.include?(job["conclusion"]) }

artifacts = []
section.call("Artifacts") do
  artifacts = gh_list("repos/#{repo}/actions/runs/#{run_id}/artifacts?per_page=100",
                      ".artifacts[] | select(.expired | not) | {id, name, size_in_bytes, created_at}")
end
reports = artifacts.select { |artifact| artifact["name"].start_with?("junit-") }

suites = []
stale = []
section.call("Test reports") do
  entries = Dir.exist?(reports_dir) ? Dir.children(reports_dir).sort : []
  dirs = entries.select { |entry| entry.start_with?("junit-") }.to_h { |entry| [entry, File.join(reports_dir, entry)] }
  # download-artifact extracts a lone matching artifact without its directory.
  dirs[reports.first["name"]] = reports_dir if dirs.empty? && reports.size == 1 && entries.any? { |e| e.end_with?(".xml") }
  suites = dirs.map do |entry, dir|
    cases, broken = parse_reports(dir)
    {name: entry.delete_prefix("junit-"), cases: cases, broken: broken}
  end
  # A re-run job that stopped before uploading leaves the report of an earlier attempt behind.
  created = reports.to_h { |artifact| [artifact["name"].delete_prefix("junit-"), artifact["created_at"]] }
  stale = suites.filter_map do |suite|
    started = jobs_by_name.dig(suite[:name], "started_at")
    suite[:name] if started && created[suite[:name]] && Time.iso8601(created[suite[:name]]) < Time.iso8601(started)
  end
  suites.reject! { |suite| stale.include?(suite[:name]) }
  suites.each { |suite| suite[:url] = jobs_by_name.dig(suite[:name], "html_url") }
end
suites_by_name = suites.to_h { |suite| [suite[:name], suite] }

failures = []
run_cancel = nil
section.call("Failure classification") do
  messages = unsuccessful.to_h do |job|
    annotations = []
    section.call("Annotations of #{job['name']}") do
      annotations = gh_list("repos/#{repo}/check-runs/#{job['id']}/annotations?per_page=100", ".[] | {message, annotation_level}")
    end
    [job["name"], annotations]
  end
  run_cancel = messages.values.flatten.map { |annotation| annotation["message"].to_s }
                       .find { |message| message.match?(CANCEL_PATTERN) }
  failures = unsuccessful.map do |job|
    classify(job, suites_by_name[job["name"]], messages[job["name"]], run_cancel, stale.include?(job["name"]))
  end
  failures.sort_by! { |failure| [CAUSES.keys.index(failure.cause), failure.job["name"]] }
end

cleanup_note = nil
section.call("Cleanup") do
  next unless ENV["SUMMARY_CLEANUP"] == "true" && !jobs.empty?

  names = artifacts.to_set { |artifact| artifact["name"] }
  obsolete = artifacts.select do |artifact|
    name = artifact["name"]
    (name.start_with?("couchbase-") && name =~ /-\d+\.\d+\z/ && names.include?(name.sub(/-\d+\.\d+\z/, ""))) ||
      (name.start_with?("junit-") && unsuccessful.empty?)
  end
  obsolete.each do |artifact|
    # A read-only token (pull requests from forks) fails every delete the same way.
    _, err = gh("api", "--method", "DELETE", "repos/#{repo}/actions/artifacts/#{artifact['id']}", attempts: 1)
    if err
      cleanup_note = "#{obsolete.size - obsolete.index(artifact)} intermediate artifacts were not deleted and " \
                     "expire with their retention period: #{err}"
      break
    end
    artifacts.delete(artifact)
  end
end

out = []
title = ENV.fetch("SUMMARY_TITLE", "Workflow")
if jobs.empty?
  out << "## ⚠️ #{title}: job results unavailable"
elsif unsuccessful.empty?
  out << "## ✅ #{title}: #{jobs.size} jobs succeeded"
else
  causes = failures.group_by(&:cause).map { |cause, list| "#{list.size} × #{CAUSES.fetch(cause)}" }
  out << if run_cancel
           "## 🛑 #{title}: cancelled. #{describe_cancel(run_cancel)}"
         else
           "## ❌ #{title}: #{unsuccessful.size} of #{jobs.size} jobs did not succeed"
         end
  out << ""
  out << causes.join(" · ")
end
out << "" << ENV["SUMMARY_SUBTITLE"] if ENV["SUMMARY_SUBTITLE"]
out << ""

section.call("Failures by cause") do
  next if failures.empty?

  out << "### Failures by cause"
  out << ""
  out << "| Cause | Job | Step | Detail |"
  out << "|---|---|---|---|"
  # Cancelled jobs with the same reason share one row: a cancelled run cancels most of the matrix.
  cancelled, others = failures.partition { |failure| failure.cause == :cancelled }
  others.each do |failure|
    out << "| #{CAUSES.fetch(failure.cause)} | #{link(failure.job['name'], failure.job['html_url'])} | " \
           "#{cell(failure.step)} | #{cell(failure.detail)} |"
  end
  cancelled.group_by(&:detail).each do |detail, group|
    names = group.map { |failure| link(failure.job["name"], failure.job["html_url"]) }.join(", ")
    out << "| #{CAUSES.fetch(:cancelled)} | #{names} | | #{cell(detail)} |"
  end
  skipped = jobs.count { |job| job["conclusion"] == "skipped" }
  out << "" << "#{skipped} jobs did not run because a job they need did not succeed." if skipped.positive? && !run_cancel
  out << ""
end

all_cases = suites.flat_map { |suite| suite[:cases].map { |tc| [suite, tc] } }
section.call("Tests") do
  # Jobs with a "Test" step that left no report, so a lost report is not a silent gap.
  missing = jobs.select { |job| job["steps"].to_a.any? { |s| s["name"] == "Test" } && !suites_by_name.key?(job["name"]) }
  next if suites.empty? && missing.empty?

  count = lambda { |cases, outcome| cases.count { |tc| tc.outcome == outcome } }
  out << "### Tests"
  out << ""
  out << "| Configuration | | Tests | Passed | Failed | Errors | Skipped | Time |"
  out << "|---|---|--:|--:|--:|--:|--:|--:|"
  rows = suites.map { |suite| [suite[:name], link(suite[:name], suite[:url]), suite[:cases], suite[:broken]] }
  rows += missing.map do |job|
    note = stale.include?(job["name"]) ? "stale report from an earlier attempt" : "no report"
    [job["name"], link(job["name"], job["html_url"]), nil, note]
  end
  rows.sort_by!(&:first)
  rows << [nil, "**Total**", all_cases.map(&:last), suites.flat_map { |suite| suite[:broken] }]
  rows.each do |_, label, cases, broken|
    if cases.nil?
      out << "| #{label} | ⚠️ | #{broken} | | | | | |"
      next
    end
    bad = count.call(cases, :failure) + count.call(cases, :error)
    icon = if bad.positive? then "❌"
           elsif cases.empty? || broken.any? then "⚠️"
           else "✅"
           end
    cells = [cases.size, count.call(cases, :passed), count.call(cases, :failure), count.call(cases, :error),
             count.call(cases, :skipped), human_time(cases.sum(&:time))]
    out << "| #{label} | #{icon} | #{cells.join(' | ')} |"
  end
  out << ""
  broken = suites.select { |suite| suite[:broken].any? }
  broken.each { |suite| out << "⚠️ #{suite[:name]}: unreadable report files #{suite[:broken].join(', ')}" }
  out << "" unless broken.empty?
end

section.call("Failed tests") do
  grouped = all_cases.select { |_, tc| BAD_OUTCOMES.include?(tc.outcome) }.group_by { |_, tc| tc.id }
  next if grouped.empty?

  ranked = grouped.sort_by { |id, hits| [-hits.size, id] }
  out << "### Failed tests"
  out << ""
  out << "| Test | Configurations |"
  out << "|---|---|"
  ranked.first(MAX_FAILED_ROWS).each do |id, hits|
    configurations =
      if hits.size > MAX_LINKED_CONFIGURATIONS then "#{hits.size} configurations"
      else hits.map { |suite, _| link(suite[:name], suite[:url]) }.join(", ")
      end
    out << "| `#{cell(id, code: true)}` | #{configurations} |"
  end
  out << "" << "#{ranked.size - MAX_FAILED_ROWS} more failed tests are not listed." if ranked.size > MAX_FAILED_ROWS
  out << ""
  ranked.first(MAX_FAILURES).each do |id, hits|
    out << "<details><summary><code>#{CGI.escapeHTML(id)}</code> (#{hits.size})</summary>"
    out << ""
    hits.first(MAX_BODIES_PER_TEST).each do |suite, tc|
      body = tc.body.lines
      body = body.first(MAX_BODY_LINES) + ["... #{body.size - MAX_BODY_LINES} more lines\n"] if body.size > MAX_BODY_LINES
      body = body.join.rstrip
      body = "#{body.byteslice(0, MAX_BODY_BYTES).scrub('')}\n... truncated" if body.bytesize > MAX_BODY_BYTES
      out << [link(suite[:name], suite[:url]), tc.file && "`#{tc.file}:#{tc.line}`"].compact.join(", ")
      out << ""
      out << "````"
      out << body.gsub("````", "''''")
      out << "````"
    end
    if hits.size > MAX_BODIES_PER_TEST
      others = hits.drop(MAX_BODIES_PER_TEST).map { |suite, _| link(suite[:name], suite[:url]) }
      out << "" << "Also failed in #{others.join(', ')}."
    end
    out << ""
    out << "</details>"
  end
  out << "" << "#{ranked.size - MAX_FAILURES} more failed tests are listed in the table only." if ranked.size > MAX_FAILURES
  out << ""

  ranked.first(MAX_ANNOTATIONS).each do |id, hits|
    tc = hits.first.last
    message = "Failed in #{hits.map { |suite, _| suite[:name] }.join(', ')}\n\n#{tc.message}"
    location = tc.file ? "file=#{property(tc.file)}," : ""
    location += "line=#{property(tc.line)}," if tc.file && tc.line
    puts "::error #{location}title=#{property(id)}::#{message.gsub('%', '%25').gsub("\r", '%0D').gsub("\n", '%0A')}"
  end
end

section.call("Slowest tests") do
  next if all_cases.empty?

  out << "<details><summary>Slowest tests</summary>"
  out << ""
  out << "| Test | Configuration | Time |"
  out << "|---|---|--:|"
  all_cases.max_by(15) { |_, tc| tc.time }.each do |suite, tc|
    out << "| `#{cell(tc.id, code: true)}` | #{link(suite[:name], suite[:url])} | #{human_time(tc.time)} |"
  end
  out << ""
  out << "</details>"
  out << ""
end

section.call("Job results") do
  next if jobs.empty?

  out << "### Jobs"
  out << ""
  out << "| Job | Result |"
  out << "|---|---|"
  jobs.group_by { |job| job["name"][/\A[^ ]+/] }.each do |base, group|
    cells = group.sort_by { |job| job["name"] }.map do |job|
      variant = job["name"][/\((.*)\)\z/, 1]
      icon = ICONS.fetch(job["conclusion"].to_s) { job["status"] == "completed" ? "❔" : "⏳" }
      link([icon, variant].compact.join(" "), job["html_url"])
    end
    out << "| `#{base}` | #{cells.join(' ')} |"
  end
  out << ""
end

section.call("Artifact list") do
  next if artifacts.empty?

  out << "### Artifacts"
  out << ""
  out << "| Artifact | Size |"
  out << "|---|--:|"
  artifacts.sort_by { |artifact| artifact["name"] }.each do |artifact|
    url = "https://github.com/#{repo}/actions/runs/#{run_id}/artifacts/#{artifact['id']}"
    out << "| #{link(artifact['name'], url)} | #{human_size(artifact['size_in_bytes'])} |"
  end
  out << ""
end
out << cleanup_note << "" if cleanup_note

unless problems.empty?
  out << "### Report problems"
  out << ""
  out << "These parts of the report could not be built:"
  out << ""
  problems.each { |problem| out << "* #{cell(problem, 500)}" }
  out << ""
end

summary = out.join("\n")
if summary.bytesize > MAX_SUMMARY_BYTES
  note = "\n\n⚠️ The report was truncated to fit the step summary limit.\n"
  # Cut between lines of out where no code fence or <details> is open. A
  # failure body is one line of out, so its text cannot open or close either.
  size = 0
  cut = 0
  fence = false
  depth = 0
  out.each_with_index do |line, index|
    cut = index if !fence && depth.zero?
    size += line.bytesize + 1
    break if size > MAX_SUMMARY_BYTES - note.bytesize

    if line == "````" then fence = !fence
    elsif fence then next
    elsif line.start_with?("<details>") then depth += 1
    elsif line == "</details>" then depth -= 1
    end
  end
  summary = out.first(cut).join("\n") + note
end
File.write(ENV.fetch("GITHUB_STEP_SUMMARY", "/dev/stdout"), summary, mode: "a")
File.write(ENV["GITHUB_OUTPUT"], "written=true\n", mode: "a") if ENV["GITHUB_OUTPUT"]
problems.each { |problem| warn(problem) }
exit(problems.empty? ? 0 : 1)
