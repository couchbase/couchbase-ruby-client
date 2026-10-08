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

# Checks every `uses:` under .github:
#
# * error:
#   - a reference to another repository not pinned as
#     "owner/repo@<sha> # <tag or branch>";
#   - a step, job or service container image without a digest;
#   - a commented tag that does not point at the pinned commit;
#   - a tag or branch that could not be checked.
# * warning: a newer release tag exists, or the commented branch has moved on.
#
# FLOATING lists references allowed to follow a tag or branch; the reason for
# each is next to the reference.
#
# References are found with the YAML parser, so every form of a `uses:` key is
# seen; the comment is read from the line of the value. A finding is reported
# once per action and pinned version, at its first reference, with the number
# of other references. Exits 1 on any error.
#
#   bin/check-action-pins.rb    # from the repository root, GH_TOKEN for gh

require "open3"
require "psych"

FILES = Dir[".github/**/*.{yml,yaml}"].freeze
PINNED = /\A(?<repo>[A-Za-z0-9][A-Za-z0-9-]*\/(?!\.+[\/@])[A-Za-z0-9._-]+)(?:\/[^@\s]+)?@(?<sha>[0-9a-f]{40})\z/
VERSION = /\Av?\d+(?:\.\d+)*\z/
BRANCH = /\A(?!.*\.\.)[A-Za-z0-9_][A-Za-z0-9._\/-]*\z/
IMAGE = /\A(?:docker:\/\/)?[^@\s]+@sha256:[0-9a-f]{64}\z/

# Workflow file => references in it that may follow a branch instead of a pin.
FLOATING = {
  ".github/workflows/fit-tests.yml" => /\Acouchbaselabs\/fit-cli\/\.github\/workflows\/fit-cli\.yaml@ci\z/,
}.freeze

Ref = Struct.new(:file, :line, :ref, :comment)

def property(value)
  value.to_s.gsub("%", "%25").gsub("\r", "%0D").gsub("\n", "%0A").gsub(":", "%3A").gsub(",", "%2C")
end

def annotate(level, refs, title, message)
  @failed = true if level == "error"
  first = refs.first
  others = refs.size - 1
  message += " (#{others} more #{others == 1 ? 'reference' : 'references'})" if others.positive?
  puts "::#{level} file=#{property(first.file)},line=#{first.line},title=#{property(title)}::" \
       "#{message.gsub('%', '%25').gsub("\r", '%0D').gsub("\n", '%0A')}"
end

# Retried, because a failed lookup fails the check.
def gh(*args)
  error = nil
  3.times do |attempt|
    out, err, status = Open3.capture3("gh", *args)
    return [out, nil] if status.success?

    error = err.strip.lines.first.to_s.strip
    break if error.include?("HTTP 404")

    sleep(5 * (attempt + 1)) if attempt < 2
  end
  [nil, error]
end

def version(tag) = Gem::Version.new(tag.delete_prefix("v"))

# Scalar values of `uses:` keys anywhere in the document.
def uses_nodes(node, found = [])
  case node
  when Psych::Nodes::Mapping
    node.children.each_slice(2) do |key, value|
      found << value if key.is_a?(Psych::Nodes::Scalar) && key.value == "uses" && value.is_a?(Psych::Nodes::Scalar)
      uses_nodes(value, found)
    end
  when Psych::Nodes::Node
    node.children&.each { |child| uses_nodes(child, found) }
  end
  found
end

def child(mapping, name)
  return unless mapping.is_a?(Psych::Nodes::Mapping)

  mapping.children.each_slice(2).find { |key, _| key.is_a?(Psych::Nodes::Scalar) && key.value == name }&.last
end

# Image nodes of `jobs.<id>.container` and `jobs.<id>.services.<id>`, in the
# string form or as their `image:`. A non-scalar image is returned as is, and
# fails the digest check.
def image_nodes(document)
  jobs = child(document.root, "jobs")
  return [] unless jobs.is_a?(Psych::Nodes::Mapping)

  jobs.children.each_slice(2).flat_map do |_, job|
    services = child(job, "services")
    containers = [child(job, "container")]
    containers += services.children.each_slice(2).map(&:last) if services.is_a?(Psych::Nodes::Mapping)
    containers.compact.filter_map { |node| node.is_a?(Psych::Nodes::Mapping) ? child(node, "image") : node }
  end
end

images = []
refs = FILES.flat_map do |file|
  lines = File.readlines(file)
  document = Psych.parse_file(file)
  image_nodes(document).each do |node|
    images << Ref.new(file, node.start_line + 1, node.is_a?(Psych::Nodes::Scalar) ? node.value : "(not a string)")
  end
  uses_nodes(document).map do |node|
    # The comment after the value when it is one word, as in "# v1.2.3"; nil otherwise.
    comment = lines[node.end_line].to_s[/\A[^#]*\S\s+#\s*(\S+)\s*\z/, 1]
    Ref.new(file, node.start_line + 1, node.value, comment)
  end
end
# This repository's own actions and workflows, and the allowed floating references.
refs.reject! { |ref| ref.ref.start_with?("./") || FLOATING[ref.file]&.match?(ref.ref) }

docker, refs = refs.partition { |ref| ref.ref.start_with?("docker://") }
(images + docker).reject { |ref| IMAGE.match?(ref.ref) }.group_by(&:ref).each do |ref, group|
  annotate("error", group, "Unpinned image", "#{ref} must be pinned as <image>@sha256:<digest>")
end

unpinned, pinned = refs.partition do |ref|
  !PINNED.match?(ref.ref) || ref.comment.nil? || !ref.comment.match?(BRANCH)
end
unpinned.group_by(&:ref).each do |ref, group|
  annotate("error", group, "Unpinned action", "#{ref} must be pinned as owner/repo@<commit sha> # <tag or branch>")
end

pinned.group_by { |ref| PINNED.match(ref.ref)[:repo] }.each do |repo, repo_refs|
  tags = nil
  tags_error = nil
  repo_refs.group_by { |ref| [PINNED.match(ref.ref)[:sha], ref.comment] }.each do |(sha, comment), group|
    if comment.match?(VERSION)
      if tags.nil? && tags_error.nil?
        out, tags_error = gh("api", "--paginate", "repos/#{repo}/tags?per_page=100",
                             "--jq", ".[] | [.name, .commit.sha] | @tsv")
        tags = out&.lines.to_h { |line| line.chomp.split("\t", 2) }
      end
      if tags_error
        annotate("error", group, "Action not checked", "#{repo}: tags unavailable: #{tags_error}")
      elsif tags[comment].nil?
        annotate("error", group, "Unknown tag", "#{repo} has no tag #{comment}")
      elsif tags[comment] != sha
        annotate("error", group, "Moved tag", "#{repo} #{comment} points at #{tags[comment]}, pinned #{sha}")
      else
        latest = tags.keys.grep(VERSION).max_by { |tag| version(tag) }
        if version(latest) > version(comment)
          annotate("warning", group, "Newer action", "#{repo} #{latest} is available, pinned #{comment}")
        end
      end
    else
      out, err = gh("api", "repos/#{repo}/commits/#{comment}", "--jq", ".sha")
      if err
        annotate("error", group, "Action not checked", "#{repo}@#{comment}: #{err}")
      elsif out.strip != sha
        annotate("warning", group, "Moved branch", "#{repo} #{comment} is at #{out.strip}, pinned #{sha}")
      end
    end
  end
end

exit(@failed ? 1 : 0)
