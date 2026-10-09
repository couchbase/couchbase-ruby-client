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

# Checks every "[<registry>/]alpine:<branch>@sha256:<digest>" image under .github against
# the Alpine releases and the ECR Public mirror of Docker Hub:
#
# * warning: the branch is not the oldest Alpine release that still has
#   security support, the tag now points at another image, or the image is not
#   pulled from the mirror;
# * error: the release list or the current digest could not be fetched.
#
# The musl gem is built in that image and loads only on the same musl or a
# newer one, so the oldest supported release gives the widest compatibility.
# Exits 1 on any error.
#
#   bin/check-alpine-image.rb    # from the repository root

require "date"
require "json"
require "net/http"

FILES = Dir[".github/**/*.{yml,yaml}"].freeze
IMAGE = /(?<name>[\w.\-\/]*\balpine):(?<branch>\d+\.\d+)@(?<digest>sha256:[0-9a-f]{64})/
# Docker Hub limits anonymous pulls per IP address, which GitHub runners share.
MIRROR = "public.ecr.aws/docker/library/alpine"
RELEASES = URI("https://alpinelinux.org/releases.json")
INDEX = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json"

def property(value)
  value.to_s.gsub("%", "%25").gsub("\r", "%0D").gsub("\n", "%0A").gsub(":", "%3A").gsub(",", "%2C")
end

def annotate(level, file, line, title, message)
  @failed = true if level == "error"
  puts "::#{level} file=#{property(file)},line=#{line},title=#{property(title)}::" \
       "#{message.gsub('%', '%25').gsub("\r", '%0D').gsub("\n", '%0A')}"
end

# Retried, because a failed lookup fails the check.
def fetch(uri, headers = {}, method: Net::HTTP::Get)
  error = nil
  3.times do |attempt|
    response = Net::HTTP.start(uri.host, uri.port, use_ssl: true, open_timeout: 10, read_timeout: 20) do |http|
      http.request(method.new(uri, headers))
    end
    return response if response.is_a?(Net::HTTPSuccess)

    error = "#{uri}: HTTP #{response.code}"
    sleep(5 * (attempt + 1)) if attempt < 2
  rescue IOError, SystemCallError, Timeout::Error, OpenSSL::SSL::SSLError => e
    error = "#{uri}: #{e.message}"
    sleep(5 * (attempt + 1)) if attempt < 2
  end
  raise error
end

def current_digest(branch)
  token = JSON.parse(fetch(URI("https://public.ecr.aws/token/")).body)["token"]
  fetch(URI("https://public.ecr.aws/v2/docker/library/alpine/manifests/#{branch}"),
        {"Authorization" => "Bearer #{token}", "Accept" => INDEX}, method: Net::HTTP::Head)["docker-content-digest"] or
    raise "alpine:#{branch}: the registry returned no digest"
end

def version(branch) = Gem::Version.new(branch)

refs = FILES.flat_map do |file|
  File.readlines(file).each_with_index.filter_map do |text, index|
    match = IMAGE.match(text)
    [file, index + 1, match[:branch], match[:digest], match[:name]] if match
  end
end
exit 0 if refs.empty?

begin
  today = Date.today
  # "3.21" => end of life date, for every numbered release branch.
  eol = JSON.parse(fetch(RELEASES).body)["release_branches"].to_h do |branch|
    [branch["rel_branch"].to_s[/\Av(\d+\.\d+)\z/, 1], Date.parse(branch["eol_date"].to_s)]
  rescue Date::Error
    [nil, nil]
  end.except(nil)
  supported = eol.select { |_, date| date >= today }.keys
  raise "#{RELEASES}: no supported release" if supported.empty?

  oldest = supported.min_by { |name| version(name) }
rescue StandardError => e
  refs.each { |file, line| annotate("error", file, line, "Alpine image not checked", e.message) }
  exit 1
end

refs.each do |file, line, _, _, name|
  annotate("warning", file, line, "Alpine image", "#{name} is not #{MIRROR}") if name != MIRROR
end

refs.group_by { |_, _, branch, digest| [branch, digest] }.each do |(branch, digest), group|
  file, line = group.first
  if !supported.include?(branch)
    reached = eol[branch] ? "reached end of life on #{eol[branch]}" : "is not a known Alpine release"
    annotate("warning", file, line, "Alpine image", "alpine:#{branch} #{reached}; build on alpine:#{oldest}")
  elsif branch != oldest
    annotate("warning", file, line, "Alpine image",
             "alpine:#{branch} is not the oldest supported Alpine release; build on alpine:#{oldest}")
  end
  begin
    latest = current_digest(branch)
    annotate("warning", file, line, "Alpine image", "alpine:#{branch} is now #{latest}, pinned #{digest}") if latest != digest
  rescue StandardError => e
    annotate("error", file, line, "Alpine image not checked", e.message)
  end
end

exit(@failed ? 1 : 0)
