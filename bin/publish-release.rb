#!/usr/bin/env ruby
# frozen_string_literal: true

# Publish a couchbase-ruby-client release.
#
# Given a tag, this script:
#   1. resolves the tag to the GitHub Actions "tests" workflow run that built it,
#   2. downloads the final gem artifacts (Octokit) and unpacks them,
#   3. generates a GPG-signed SHA256 checksum file,
#   4. uploads the gems + checksums to the versioned S3 path,
#   5. rebuilds the S3-hosted RubyGems repository index (and invalidates CloudFront),
#   6. downloads the docs artifact and syncs it to the docs S3 bucket,
#   7. prints instructions to verify the release straight from the S3 repo, and
#   8. after you type the release version to confirm, pushes the gems to
#      RubyGems.org (public, irreversible — a mistaken push can only be yanked,
#      never deleted, and its version number is burned forever).
#
# RubyGems is intentionally LAST: S3 can be re-synced or deleted, a RubyGems push
# can only be yanked. Nothing is pushed to RubyGems without a confirmation prompt,
# so you can install from the S3 repo and smoke-test the gems first.
#
# It runs in DRY-RUN mode by default: nothing is published unless --execute is
# given. Even with --execute it refuses to overwrite an already-published gem or
# an existing sdk-<version>/ S3 prefix (override with --force).
#
# GitHub token and S3 settings are read from the couchbase XDG config file:
#   ${XDG_CONFIG_HOME:-~/.config}/couchbase/ruby-sdk.yml
# RubyGems credentials are NOT stored there; `gem push` uses its own
# (Gem.configuration.credentials_path).
#
# Prerequisites (each maintainer sets these up on their own machine):
#   * Ruby with the `octokit` gem      (gem install octokit)
#   * CLI tools on PATH: curl, unzip, gpg, aws   (the script checks these up front)
#   * An AWS profile with write access to the packages/docs S3 buckets
#   * A GPG signing key (see "Signing" below)
#   * A RubyGems API key with the "Push rubygem" scope, from
#       https://rubygems.org/settings/edit  (then `gem signin`)
#
# Secrets: everything sensitive lives ONLY in the config file above, which the
# script creates mode 0600 (owner-only). Get it from a teammate over a secure
# channel and NEVER commit it. `--config=print` redacts secret values so its
# output is safe to paste/screen-share; the file on disk is not. No secret is
# written into any published artifact, log line, or the working directory.
#
# Signing: step 3 clearsigns the checksums with YOUR default GPG key, so the
# signature published to S3 carries your key's identity — this is intentional
# release provenance. Each maintainer signs with their own key; there is nothing
# machine- or person-specific baked into the script itself.

# Stdlib — always present with a normal Ruby install.
require "fileutils"
require "optparse"
require "yaml"
require "json"
require "open-uri"
require "net/http"
require "open3"
require "shellwords"
require "digest"
require "tmpdir"
require "rubygems/package"

# Third-party gems — not stdlib, so fail early with install instructions rather
# than a bare LoadError halfway into someone's first run.
begin
  require "octokit"
rescue LoadError
  warn "ERROR: the 'octokit' gem is required but not installed."
  warn "Install it with one of:"
  warn "  gem install octokit"
  warn "  bundle add octokit    # if you run this via Bundler"
  exit 1
end

CONFIG_DIR = File.join(ENV.fetch("XDG_CONFIG_HOME", File.join(Dir.home, ".config")), "couchbase")
DEFAULT_CONFIG_PATH = File.join(CONFIG_DIR, "ruby-sdk.yml")

# Released-gem cache lives under the XDG data dir, partitioned per bucket/path so
# repositories with different layouts never share a cache.
DATA_DIR = File.join(ENV.fetch("XDG_DATA_HOME", File.join(Dir.home, ".local", "share")), "couchbase", "ruby-sdk-release")

CONFIG_TEMPLATE = {
  "github" => {
    "token" => "YOUR_GITHUB_TOKEN_HERE",
    "repo" => "couchbase/couchbase-ruby-client",
    # Workflow file (or display name) that produces the gem artifacts.
    "workflow" => "tests.yml",
  },
  # No rubygems section: `gem push` reads its own credentials
  # (Gem.configuration.credentials_path — ~/.gem/ or the XDG gem dir).
  "aws" => {
    "profile" => nil,                       # optional; leave empty to use the default AWS profile
    "packages_bucket" => nil,               # S3 bucket hosting the gem repository
    "packages_repo_path" => "/clients/ruby", # path within the bucket
    "packages_cloudfront_dist_id" => nil,   # optional; leave empty to skip invalidation
    "packages_base_url" => nil,             # optional; public HTTPS base (CDN/custom domain) for verify instructions
    "docs_bucket" => nil,                    # S3 bucket hosting the HTML docs (e.g. docs.couchbase.com)
    "docs_repo_path" => "/sdk-api",          # path within the docs bucket; docs land under <path>/couchbase-ruby-client-<version>/
    "docs_cloudfront_dist_id" => nil,        # optional; leave empty to skip invalidation
  },
}.freeze

def abort!(msg)
  warn "ERROR: #{msg}"
  exit 1
end

# Verify a non-stdlib CLI tool is on PATH; otherwise abort with an install hint.
def require_command!(cmd, hint)
  return if system("command -v #{Shellwords.escape(cmd)} > /dev/null 2>&1")

  abort!("required command '#{cmd}' not found on PATH.\n  Install it: #{hint}")
end

def prompt(question)
  print question
  $stdin.gets.to_s.strip
end

# Download a URL to a path with curl, WITHOUT echoing the URL: GitHub artifact
# download URLs are presigned and carry a short-lived read credential, so keeping
# them out of stdout/logs matters. Aborts on failure.
def download_file!(url, dest)
  system("curl", "-fsSL", "-o", dest, url) || abort!("download failed for #{File.basename(dest)}")
end

# Write the config owner-only (0600): it holds the GitHub token, so it must not be
# world-readable on a shared machine.
def write_config!(path, data)
  FileUtils.mkdir_p(File.dirname(path))
  File.write(path, data)
  File.chmod(0o600, path)
end

# Command runner that honours dry-run. In dry-run it only prints; otherwise it
# prints and executes, aborting on failure. Args are passed as an array so no
# shell quoting/injection is involved.
class Runner
  def initialize(dry_run:)
    @dry_run = dry_run
  end

  def run(*args, env: {}, chdir: nil, allow_fail: false)
    pretty = mask_args(args)
    pretty = "#{env.map { |k, v| "#{k}=#{mask(k, v)}" }.join(' ')} #{pretty}" unless env.empty?
    puts "  #{@dry_run ? '[dry-run] would run' : '$'} #{pretty}"
    return true if @dry_run

    opts = {}
    opts[:chdir] = chdir if chdir
    # Inherit our stdin/stdout/stderr so interactive prompts (e.g. gem push asking
    # for an MFA OTP) reach the user. Only pass env when non-empty to avoid the
    # empty-hash edge in system(env, ...).
    ok = env.empty? ? system(*args, **opts) : system(env, *args, **opts)
    return ok if allow_fail

    abort!("command failed: #{pretty}") unless ok
    ok
  end

  def dry_run?
    @dry_run
  end

  private

  def mask(key, value)
    /KEY|TOKEN|SECRET|PASSWORD/i.match?(key.to_s) ? "***" : value
  end

  # Escape args for display, masking the value that follows a secret flag (--otp)
  # so a one-time code is never echoed to the terminal or a log.
  def mask_args(args)
    mask_next = false
    args.map do |a|
      if mask_next
        mask_next = false
        "***"
      else
        mask_next = (a == "--otp")
        Shellwords.escape(a)
      end
    end.join(" ")
  end
end

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(path)
  unless File.exist?(path)
    write_config!(path, CONFIG_TEMPLATE.to_yaml)
    abort!("created default config at #{path} — fill it in (or run --config=generate) and re-run")
  end

  config = YAML.load_file(path)
  token = config.dig("github", "token")
  unless token.is_a?(String) && token.match?(/\A(ghp_|github_pat_|gho_|ghu_|ghs_|ghr_)[A-Za-z0-9_]{20,}\z/)
    abort!("github.token in #{path} is not a valid GitHub token")
  end
  config
end

# --config=print (or bare --config): show the existing config with secret values
# redacted, so the output is safe to paste into a chat or screen-share. Read the
# file itself if you need the raw token.
def print_config(path)
  abort!("no config at #{path}") unless File.exist?(path)
  puts "# #{path} (secret values redacted)"
  puts File.read(path).gsub(/^(\s*[\w-]*(?:token|secret|key|password)[\w-]*\s*:).*/i, '\1 "***redacted***"')
end

# --config=edit: open the config in $VISUAL/$EDITOR (seeding the template if missing).
def edit_config(path)
  write_config!(path, CONFIG_TEMPLATE.to_yaml) unless File.exist?(path)
  editor = [ENV.fetch("VISUAL", nil), ENV.fetch("EDITOR", nil)].find { |e| e && !e.empty? } || "vi"
  system(*editor.split, path) || abort!("editor exited with an error")
end

# --config=generate: build a config, guessing values from the environment and
# confirming each one interactively.
def generate_config(path)
  existing = File.exist?(path) ? (YAML.load_file(path) || {}) : {}

  git_repo = `git config --get remote.origin.url 2>/dev/null`[/[:\/]([^\/:]+\/[^\/]+?)(?:\.git)?\s*\z/, 1]
  gh_guess = existing.dig("github", "token")

  # `secret: true` shows only a masked hint for the default (never the full token),
  # while still keeping the real existing value when the answer is left blank.
  ask = lambda do |label, default, secret: false|
    shown = secret && default ? "#{default[0, 4]}…(keep existing)" : default
    print default ? "#{label} [#{shown}]: " : "#{label}: "
    ans = $stdin.gets&.strip
    ans.nil? || ans.empty? ? default : ans
  end

  t = CONFIG_TEMPLATE
  config = {
    "github" => {
      "token" => ask.call("GitHub token", gh_guess, secret: true),
      "repo" => ask.call("GitHub repo", git_repo || existing.dig("github", "repo") || t["github"]["repo"]),
      "workflow" => ask.call("Workflow file", existing.dig("github", "workflow") || t["github"]["workflow"]),
    },
    "aws" => {
      "profile" => ask.call("AWS profile", existing.dig("aws", "profile") || t["aws"]["profile"]),
      "packages_bucket" => ask.call("Packages S3 bucket", existing.dig("aws", "packages_bucket") || t["aws"]["packages_bucket"]),
      "packages_repo_path" => ask.call("Packages repo path", existing.dig("aws", "packages_repo_path") || t["aws"]["packages_repo_path"]),
      "packages_cloudfront_dist_id" => ask.call("Packages CloudFront distribution id (empty to skip invalidation)",
                                                existing.dig("aws",
                                                             "packages_cloudfront_dist_id") || t["aws"]["packages_cloudfront_dist_id"]),
      "packages_base_url" => ask.call("Packages public base URL (for verify instructions)",
                                      existing.dig("aws", "packages_base_url") || t["aws"]["packages_base_url"]),
      "docs_bucket" => ask.call("Docs S3 bucket", existing.dig("aws", "docs_bucket") || t["aws"]["docs_bucket"]),
      "docs_repo_path" => ask.call("Docs repo path", existing.dig("aws", "docs_repo_path") || t["aws"]["docs_repo_path"]),
      "docs_cloudfront_dist_id" => ask.call("Docs CloudFront distribution id (empty to skip invalidation)",
                                            existing.dig("aws", "docs_cloudfront_dist_id") || t["aws"]["docs_cloudfront_dist_id"]),
    },
  }

  write_config!(path, config.to_yaml)
  puts "\nWrote #{path}"
end

# ---------------------------------------------------------------------------
# GitHub
# ---------------------------------------------------------------------------

# Resolve a tag to the commit SHA it points at (dereferencing annotated tags).
def tag_to_sha(client, repo, tag)
  ref = client.ref(repo, "tags/#{tag}")
  obj = ref.object
  obj.type == "tag" ? client.tag(repo, obj.sha).object.sha : obj.sha
rescue Octokit::NotFound
  abort!("tag #{tag} not found in #{repo}")
end

# No run for this tag in this repo (e.g. a fork that never built it). Offer to
# trigger the workflow, then exit. workflow_dispatch runs against a ref (the tag),
# not a raw SHA, and needs the workflow to declare `on: workflow_dispatch`.
def offer_dispatch(client, repo, tag, workflow, sha)
  warn "No '#{workflow}' run found for tag #{tag} (sha #{sha}) in #{repo}"
  abort!("aborted (or pass --run-id to use a specific run)") unless prompt("Trigger '#{workflow}' for #{tag} now? [y/N] ").match?(/\A[Yy]/)
  client.workflow_dispatch(repo, workflow, tag)
  puts "Dispatched '#{workflow}' for #{tag}. Watch it at:"
  puts "  https://github.com/#{repo}/actions/workflows/#{workflow}"
  puts "Wait for it to finish, then run this script again."
  exit 1
rescue Octokit::ClientError => e
  abort!("could not dispatch '#{workflow}' for #{tag} in #{repo} " \
         "(the workflow must declare `on: workflow_dispatch` on that ref): #{e.message}")
end

# Find the workflow run that built this tag. Works for both branch pushes (the
# usual case) and a future tag-triggered run, because both land a run on the same
# head SHA. If the run exists but did not succeed, offer to re-run it (the
# equivalent of `gh run rerun <id>`) and exit so the build can finish first.
def resolve_run_id(client, repo, tag, workflow)
  sha = tag_to_sha(client, repo, tag)
  puts "Tag #{tag} -> #{sha}"
  runs = client.repository_workflow_runs(repo, head_sha: sha).workflow_runs
  runs = runs.select { |r| r.path&.end_with?(workflow) || r.name == File.basename(workflow, ".*") } if workflow && !workflow.empty?
  run = runs.max_by(&:created_at)
  offer_dispatch(client, repo, tag, workflow, sha) unless run

  if run.conclusion == "success"
    puts "Resolved workflow run ##{run.id} (#{run.html_url})"
    return run.id
  end

  state = run.status == "completed" ? run.conclusion : "#{run.status} (in progress)"
  warn "Workflow run ##{run.id} for #{tag} did not succeed: #{state}"
  warn "  #{run.html_url}"
  case prompt("Download artifacts from this run anyway, re-run it, or abort? [d/r/a] ").downcase
  when "d"
    puts "Proceeding with artifacts from run ##{run.id} (may be incomplete)"
    run.id
  when "r"
    client.rerun_workflow_run(repo, run.id)
    puts "Re-run requested for ##{run.id}. Watch it at:"
    puts "  #{run.html_url}"
    puts "Wait for it to finish, then run this script again."
    exit 1
  else
    abort!("aborted")
  end
end

# A final, shippable gem artifact. The build marks throwaway artifacts (the
# per-Ruby intermediate builds, scripts, tests, logs) with `retention-days: 1`,
# while the publishable gems keep the long default retention — so retention, not
# a name pattern, tells them apart. The `couchbase-` prefix then keeps gems while
# excluding the equally long-lived docs/otel-docs bundles. The release version is
# NOT taken from the artifact names here (the tag may differ from the gem
# version); it is read from the gems themselves after unpacking.
INTERMEDIATE_MAX_RETENTION_DAYS = 2

def final_gem_artifact?(artifact)
  return false unless artifact.name.start_with?("couchbase-")

  (artifact.expires_at - artifact.created_at) / 86_400.0 > INTERMEDIATE_MAX_RETENTION_DAYS
end

def download_artifacts(client, repo, run_id, dest)
  # Scope the download cache by run id: artifact names repeat across runs
  # (couchbase-3.8.0, …), so without this a rerun would reuse a stale zip.
  dest = File.join(dest, run_id.to_s)
  FileUtils.mkdir_p(dest)
  artifacts = client.workflow_run_artifacts(repo, run_id)[:artifacts]
  selected = artifacts.select { |a| final_gem_artifact?(a) }

  if selected.empty?
    abort!("no final gem artifacts in run #{run_id}; " \
           "available: #{artifacts.map(&:name).join(', ')}")
  end

  # GitHub expires Actions artifacts (~90 days). Expired ones return 410 on
  # download, so detect them from metadata up front rather than crashing.
  expired, live = selected.partition(&:expired)
  expired.each { |a| warn "  #{a.name}: EXPIRED (#{a.expires_at})" }
  if live.empty?
    abort!("all gem artifacts for run #{run_id} have expired (GitHub retention). " \
           "Re-run the workflow to regenerate them (run this script again and choose 'r').")
  end

  puts "Downloading #{live.size} artifact(s) (skipping #{artifacts.size - live.size}):"
  live.each do |artifact|
    zip = File.join(dest, "#{artifact.name}.zip")
    if File.exist?(zip)
      puts "  #{artifact.name}: cached"
    else
      puts "  #{artifact.name}: downloading"
      # Always fetch — the local copy is just a download cache, not a publish target.
      download_file!(client.artifact_download_url(repo, artifact.id), zip)
    end
  rescue Octokit::Deprecated, Octokit::ClientError => e
    abort!("#{artifact.name}: download failed (#{e.response_status} — expired or unavailable). " \
           "Re-run the workflow to regenerate artifacts.")
  end
  Dir[File.join(dest, "*.zip")]
end

# ---------------------------------------------------------------------------
# Local staging: unpack zips, collect gems, sign checksums
# ---------------------------------------------------------------------------

# Unpack the gem zips and determine the release version from the couchbase gem
# itself (the source `couchbase-X.Y.Z.gem`), not from the tag. Returns [gems,
# version]. All `couchbase` gems must agree on the version; the opentelemetry gem
# is versioned independently and is not checked here.
def unpack_gems(zips, staging)
  FileUtils.mkdir_p(staging)
  zips.each do |zip|
    Runner.new(dry_run: false).run("unzip", "-o", "-j", zip, "*.gem", "-d", staging)
  rescue SystemExit
    # `unzip` exits 11 when a zip has no .gem member; ignore those rather than aborting.
    warn "  note: #{File.basename(zip)} contained no .gem, skipped"
  end

  gems = Dir[File.join(staging, "*.gem")]
  abort!("no .gem files unpacked into #{staging}") if gems.empty?

  versions = gems.map { |g| Gem::Package.new(g).spec }
                 .select { |s| s.name == "couchbase" }
                 .map { |s| s.version.to_s }.uniq
  abort!("no couchbase gem among the artifacts") if versions.empty?
  abort!("inconsistent couchbase versions in artifacts: #{versions.join(', ')}") if versions.size > 1

  [gems, versions.first]
end

# Port of calculate-package-sha256.sh: sha256sum-compatible lines, GPG clearsigned.
def sign_checksums(gems, staging, version)
  out = File.join(staging, "couchbase-#{version}.sha256.txt")
  lines = gems.sort.map { |g| "#{Digest::SHA256.file(g).hexdigest}  #{File.basename(g)}" }
  tmp = File.join(staging, ".sha256.tmp")
  File.write(tmp, "#{lines.join("\n")}\n")
  # Clearsigning is a local, non-destructive operation — do it even in dry-run so
  # the full artifact set can be inspected.
  Runner.new(dry_run: false).run("gpg", "--batch", "--yes", "--clearsign", "--output", out, tmp)
  File.delete(tmp)
  puts "Signed checksums: #{out}"
  out
end

# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

# The published version entry (incl. its sha256 in "sha") for this name+platform,
# or nil if that exact version is not on rubygems.org yet. Only a genuine 404 maps
# to nil; every other failure (500, timeout, DNS) is re-raised so the caller can
# refuse to push without a definite answer — we must never push blind.
def published_rubygems_version(spec)
  data = JSON.parse(URI.open("https://rubygems.org/api/v1/versions/#{spec.name}.json").read)
  data.find { |v| v["number"] == spec.version.to_s && v["platform"] == spec.platform.to_s }
rescue OpenURI::HTTPError => e
  raise unless e.io.status.first.to_s == "404"

  nil # 404 => gem (or version line) not published yet
end

# Query the public index once per gem and partition into: to_push (not yet live),
# skip (live with a MATCHING checksum), and mismatch (live with a DIFFERENT
# checksum). A mismatch means the version already on rubygems.org was built from
# different bytes than the file in hand — a red flag that stops everything, since
# the live version can never be replaced. Any failure to reach rubygems aborts too.
def plan_rubygems_push(gems)
  to_push = []
  skip = []
  mismatch = []
  gems.each do |gem|
    spec = Gem::Package.new(gem).spec
    local = Digest::SHA256.file(gem).hexdigest
    published =
      begin
        published_rubygems_version(spec)
      rescue StandardError => e
        abort!("could not verify #{File.basename(gem)} against rubygems.org (#{e.class}: #{e.message}) — " \
               "refusing to push without confirming what is already live")
      end
    if published.nil?
      to_push << [gem, spec, local]
    elsif published["sha"] && published["sha"] != local
      mismatch << [gem, spec, local, published["sha"]]
    else
      skip << [gem, spec]
    end
  end
  [to_push, skip, mismatch]
end

def push_to_rubygems(gems, version, runner)
  # Last step on purpose: a RubyGems push is public and effectively irreversible
  # (yank only), so everything else is staged first.
  puts "\n== RubyGems.org#{' (dry-run — querying only, nothing is pushed)' if runner.dry_run?} =="

  # `gem push` resolves its own credentials via Gem.configuration.credentials_path
  # (~/.gem/credentials, else $XDG_DATA_HOME/gem or ~/.local/share/gem). Check it is
  # present so we fail early rather than mid-publish; only a warning in dry-run.
  creds = Gem.configuration.credentials_path
  unless File.exist?(creds)
    msg = "no RubyGems credentials at #{creds} — run `gem signin` before publishing"
    runner.dry_run? ? warn("  NOTE: #{msg}") : abort!(msg)
  end

  to_push, skip, mismatch = plan_rubygems_push(gems)

  # Hard stop: a live version whose bytes differ from ours must never be papered
  # over. Push nothing until a human has investigated.
  unless mismatch.empty?
    warn "REFUSING TO PUSH — these versions are already on rubygems.org with a DIFFERENT checksum:"
    mismatch.each do |gem, spec, local, remote|
      warn "  #{spec.name} #{spec.version} #{spec.platform} (#{File.basename(gem)})"
      warn "    published sha256: #{remote}"
      warn "    local sha256:     #{local}"
    end
    abort!("live gems differ from the built ones — investigate before publishing (nothing was pushed)")
  end

  skip.each_value { |spec| puts "  already live (checksum matches), skipping: #{spec.name} #{spec.version} #{spec.platform}" }

  if to_push.empty?
    puts "Nothing to push — every gem is already on rubygems.org."
    return
  end

  puts "\nWill push #{to_push.size} gem(s) to rubygems.org (PUBLIC, irreversible — yank only):"
  to_push.each do |gem, spec, local|
    puts "  #{spec.name} #{spec.version} #{spec.platform}  [sha256 #{local[0, 16]}…]  #{File.basename(gem)}"
  end

  if runner.dry_run?
    to_push.each { |gem, _spec, _local| runner.run("gem", "push", gem) } # prints "[dry-run] would run"
    return
  end

  # Typed confirmation: retyping the exact version defeats a reflexive "y" on the
  # wrong release. Anything else declines cleanly (nothing pushed, S3 untouched).
  if prompt("\nThis CANNOT be undone. Type the release version #{version.inspect} to publish: ") != version
    puts "Not confirmed — nothing pushed to RubyGems. Gems are staged on S3; verify, then " \
         "re-run with --skip s3-version,s3-repo to publish."
    return
  end

  # Let gem push handle MFA itself: when your account requires an OTP (or a WebAuthn
  # key), gem push prompts for it interactively, and it inherits our terminal via
  # system(). We do NOT pass --otp: it makes no difference to an authorization
  # failure, and pre-collecting a code breaks the WebAuthn flow.
  puts "\nPushing to RubyGems (gem push will prompt for an MFA one-time code if your account needs one)."
  puts "Using credentials: #{Gem.configuration.credentials_path}"
  to_push.each do |gem, spec, _local|
    next if runner.run("gem", "push", gem, allow_fail: true)

    warn "\n'gem push' was denied by RubyGems. \"Access Denied\" is an AUTHORIZATION failure,"
    warn "not a bad OTP (the code, if any, was accepted). Likely causes:"
    warn "  • the API key in #{Gem.configuration.credentials_path} is invalid/revoked, or"
    warn "  • it lacks the \"Push rubygem\" scope, or your account does not own #{spec.name}."
    warn "Fix:"
    warn "  1. Create a key with the \"Push rubygem\" scope at https://rubygems.org/settings/edit"
    warn "     (enable MFA on the key if your account enforces MFA for the API), then run `gem signin`."
    warn "  2. Confirm ownership:  gem owner #{spec.name}"
    warn "  3. Re-run this script; already-pushed gems are skipped automatically."
    abort!("aborted after RubyGems denied the push of #{File.basename(gem)}")
  end
  puts "Pushed #{to_push.size} gem(s) to rubygems.org."
end

# `profile` is optional: when unset the AWS CLI uses its own resolution
# (the [default] profile, AWS_PROFILE, or an instance role).
def profile_args(aws)
  p = aws["profile"]
  p.is_a?(String) && !p.empty? ? ["--profile", p] : []
end

# Whether the S3 prefix already holds objects. Critically, this distinguishes an
# empty listing (no such prefix -> false, safe to write) from an actual AWS
# failure (bad credentials, no such bucket, network) -> abort. The old version
# piped stderr to /dev/null and read any empty output as "does not exist", so an
# auth failure silently disarmed the overwrite guard.
def s3_prefix_exists?(uri, aws)
  out, err, status = Open3.capture3("aws", "s3", "ls", uri, *profile_args(aws))
  return true if status.success? && !out.strip.empty?
  return false if status.success?      # exit 0, empty listing
  return false if err.strip.empty?     # non-zero + no message = prefix simply has no keys

  abort!("could not list #{uri} (aws s3 ls exited #{status.exitstatus}): #{err.strip}")
end

def require_aws!(aws, *keys)
  keys = %w[packages_bucket packages_repo_path] if keys.empty?
  missing = keys.reject { |k| aws[k].is_a?(String) && !aws[k].empty? }
  abort!("aws.#{missing.join(', aws.')} not set in config — fill them in or --skip that stage") unless missing.empty?
end

def upload_versioned(gems, checksums, version, aws, force, runner)
  require_aws!(aws)
  puts "\n== S3 versioned path =="
  uri = "s3://#{aws['packages_bucket']}#{aws['packages_repo_path']}/sdk-#{version}/"
  if s3_prefix_exists?(uri, aws)
    msg = "#{uri} already exists"
    abort!("#{msg} — refusing to overwrite (use --force)") unless force
    warn "  WARNING: #{msg}; --force given, proceeding"
  end

  Dir.mktmpdir do |src|
    (gems + [checksums]).each { |f| FileUtils.cp(f, src) }
    runner.run("aws", "s3", "sync", "#{src}/", uri, *profile_args(aws), "--acl", "public-read")
  end
end

# Local cache of released packages for a given bucket/path, under the XDG data
# dir. The directory name is a hash of the gems URI so different buckets/paths
# never share a cache.
def gem_cache_dir(aws)
  uri = "s3://#{aws['packages_bucket']}#{aws['packages_repo_path']}/gems"
  File.join(DATA_DIR, "gem-cache", Digest::SHA256.hexdigest(uri)[0, 16])
end

# Rebuild the repository from the full set of released packages plus the new
# version, then regenerate the index locally. We only fetch the released *.gem
# packages (never the indexes — those we regenerate) and keep them in a persistent
# per-bucket cache, so the index always covers every published version.
def update_gem_repo(gems, repo_dir, aws, runner)
  require_aws!(aws)
  puts "\n== S3 RubyGems repository index =="
  s3_uri = "s3://#{aws['packages_bucket']}#{aws['packages_repo_path']}"
  cache = gem_cache_dir(aws)
  gems_dir = File.join(repo_dir, "gems")
  FileUtils.mkdir_p(cache)
  FileUtils.mkdir_p(gems_dir)

  # Refresh the cache of already-released packages. Read-only against the bucket
  # (downloads only), so it runs even in dry-run to keep the cache current and let
  # the local repo be inspected. Never --delete: the cache is additive.
  puts "Refreshing released-gem cache for #{s3_uri}/gems/ at #{cache}"
  Runner.new(dry_run: false).run("aws", "s3", "sync", "#{s3_uri}/gems/", "#{cache}/",
                                 "--exclude", "*", "--include", "*.gem", *profile_args(aws))

  # Never silently rewrite an already-released package: if a gem with the same
  # filename is already in the repo but its bytes differ from the one we are about
  # to publish, stop. A published version must be immutable.
  gems.each do |g|
    cached = File.join(cache, File.basename(g))
    next unless File.exist?(cached)
    next if Digest::SHA256.file(cached).hexdigest == Digest::SHA256.file(g).hexdigest

    abort!("#{File.basename(g)} is already in the S3 repo with DIFFERENT bytes — refusing to " \
           "overwrite a released package (published versions must never change). Cache: #{cache}")
  end

  # generate_index reads .gem files from <repo_dir>/gems: cached releases first,
  # then the new version on top.
  Dir[File.join(cache, "*.gem")].each { |g| FileUtils.cp(g, gems_dir) }
  gems.each { |g| FileUtils.cp(g, gems_dir) }
  puts "Repository holds #{Dir[File.join(gems_dir, '*.gem')].size} gem(s) (#{gems.size} new)"

  # Validate every gem the index is built from; a truncated or corrupt download in
  # the cache would otherwise yield a silently broken index served to all users.
  Dir[File.join(gems_dir, "*.gem")].each do |g|
    Gem::Package.new(g).spec
  rescue StandardError => e
    abort!("corrupt gem in repository staging: #{File.basename(g)} (#{e.class}: #{e.message}) — " \
           "delete it from the cache (#{cache}) and re-run")
  end

  unless system("gem help generate_index >/dev/null 2>&1")
    Runner.new(dry_run: false).run("gem", "install", "rubygems-generate_index", "--no-document")
  end
  Runner.new(dry_run: false).run("gem", "generate_index", "--directory", repo_dir)

  # Upload the regenerated index (specs, names, versions, info/, quick/) — it must
  # always be refreshed. Exclude gems/: the already-released .gem files came from
  # S3 via the cache and are byte-identical, so a plain sync would re-upload them
  # only because their local mtime is newer (sync compares size+mtime, not content).
  puts "Uploading regenerated index to #{s3_uri}/"
  runner.run("aws", "s3", "sync", "#{repo_dir}/", "#{s3_uri}/",
             "--exclude", "gems/*", "--acl", "public-read", *profile_args(aws))

  # Upload only the new gems, and only those not already on S3. The cache was just
  # synced down from s3://.../gems/, so a gem present there is already published
  # byte-for-byte (a same-name cached copy with DIFFERENT bytes aborted above).
  # ponytail: assumes the repo is append-only (published versions are never removed
  # from S3), which is exactly the guarantee a permanent gem repo provides — so
  # every index-referenced old gem is still on S3 and needs no re-upload.
  new_gems = gems.reject { |g| File.exist?(File.join(cache, File.basename(g))) }
  (gems - new_gems).each { |g| puts "  #{File.basename(g)}: already on S3 (identical) — skipping upload" }
  puts "Uploading #{new_gems.size} new gem(s) to #{s3_uri}/gems/"
  new_gems.each do |g|
    runner.run("aws", "s3", "cp", g, "#{s3_uri}/gems/#{File.basename(g)}",
               "--acl", "public-read", *profile_args(aws))
  end

  dist = aws["packages_cloudfront_dist_id"]
  return if dist.nil? || dist.empty?

  rp = aws["packages_repo_path"]
  runner.run("aws", "cloudfront", "create-invalidation",
             "--distribution-id", dist, *profile_args(aws),
             "--paths",
             "#{rp}/specs.4.8.gz", "#{rp}/latest_specs.4.8.gz", "#{rp}/prerelease_specs.4.8.gz",
             "#{rp}/names", "#{rp}/versions", "#{rp}/info/*", "#{rp}/quick/*")
end

# Download and unpack the docs artifact (docs-<version>) for this run. The zip
# already holds a couchbase-ruby-client-<version>/ tree laid out exactly as it
# should appear under the docs prefix, so it is uploaded as-is. Aborts if the
# artifact is missing or expired rather than skipping silently — pass --skip docs
# to omit docs on purpose.
def prepare_docs(client, repo, run_id, version, work_dir)
  name = "docs-#{version}"
  artifact = client.workflow_run_artifacts(repo, run_id)[:artifacts].find { |a| a.name == name }
  abort!("no docs artifact (#{name}) in run #{run_id} — pass --skip docs to publish without docs") unless artifact
  abort!("docs artifact #{name} has expired (#{artifact.expires_at}); re-run the workflow, or --skip docs") if artifact.expired

  dest = File.join(work_dir, "docs", run_id.to_s)
  FileUtils.mkdir_p(dest)
  zip = File.join(dest, "#{name}.zip")
  if File.exist?(zip)
    puts "Docs artifact #{name}: cached"
  else
    puts "Docs artifact #{name}: downloading"
    download_file!(client.artifact_download_url(repo, artifact.id), zip)
  end

  Runner.new(dry_run: false).run("unzip", "-o", "-q", zip, "-d", dest)
  tree = File.join(dest, "couchbase-ruby-client-#{version}")
  abort!("#{name}.zip did not contain couchbase-ruby-client-#{version}/") unless File.directory?(tree)
  tree
rescue Octokit::Deprecated, Octokit::ClientError => e
  abort!("docs artifact #{name}: download failed (#{e.response_status} — expired or unavailable). " \
         "Re-run the workflow to regenerate it, or --skip docs.")
end

# Sync the unpacked docs tree to s3://<docs_bucket><docs_repo_path>/couchbase-ruby-client-<version>/.
# Refuses to overwrite an existing version prefix without --force, mirroring the
# gem stages (a published version's docs should be immutable).
def publish_docs(tree, version, aws, force, runner)
  require_aws!(aws, "docs_bucket", "docs_repo_path")
  puts "\n== S3 docs =="
  uri = "s3://#{aws['docs_bucket']}#{aws['docs_repo_path']}/couchbase-ruby-client-#{version}/"
  if s3_prefix_exists?(uri, aws)
    msg = "#{uri} already exists"
    abort!("#{msg} — refusing to overwrite (use --force)") unless force
    warn "  WARNING: #{msg}; --force given, proceeding"
  end

  # --delete makes the version prefix an exact mirror of the built docs (removing
  # leftovers from an earlier partial/forced upload). Safe: the prefix is
  # version-specific and normally empty when we reach here.
  runner.run("aws", "s3", "sync", "#{tree}/", uri, *profile_args(aws), "--acl", "public-read", "--delete")

  dist = aws["docs_cloudfront_dist_id"]
  unless dist.nil? || dist.empty?
    runner.run("aws", "cloudfront", "create-invalidation", "--distribution-id", dist, *profile_args(aws),
               "--paths", "#{aws['docs_repo_path']}/couchbase-ruby-client-#{version}/*")
  end

  puts "Docs URL: https://#{aws['docs_bucket']}#{aws['docs_repo_path']}/couchbase-ruby-client-#{version}/index.html"
end

# Once the S3 repo is rebuilt, print how to install the gems straight from it so
# the release can be smoke-tested BEFORE the irreversible RubyGems push. Uses
# packages_base_url when set; otherwise a best-effort S3 endpoint the user may
# need to swap for the CloudFront/custom domain that actually serves the repo.
def print_verification(gems, version, aws)
  rp = aws["packages_repo_path"]
  base = aws["packages_base_url"]
  if base.is_a?(String) && !base.empty?
    source = "#{base.chomp('/')}#{rp}"
    note = nil
  else
    source = "https://#{aws['packages_bucket']}.s3.amazonaws.com#{rp}"
    note = "NOTE: aws.packages_base_url is not set — the URL above is a best-effort S3 endpoint. " \
           "Swap in the CloudFront/custom domain if that is what serves the repo."
  end

  # One entry per name+version; platform-specific variants resolve automatically.
  pairs = gems.map do |g|
    s = Gem::Package.new(g).spec
    [s.name, s.version.to_s]
  end.uniq.sort

  puts "\n== Verify from the S3 repo BEFORE pushing to RubyGems =="
  puts "Repo source: #{source}"
  warn note if note
  puts "\nMinimal Gemfile (in an empty directory):"
  puts "  source \"https://rubygems.org\""
  puts "  source \"#{source}\" do"
  pairs.each { |name, ver| puts "    gem #{name.inspect}, #{ver.inspect}" }
  puts "  end"
  puts "\n  $ bundle install"
  puts "  $ bundle exec ruby -e 'require \"couchbase\"; puts Couchbase::VERSION'"
  puts "\nOr without bundler:"
  pairs.each { |name, ver| puts "  $ gem install #{name} -v #{ver} --source #{source}/" }
  puts "\nExpected couchbase version: #{version}"
end

# Human-readable download-table label per gem platform. Unknown platforms fall
# back to the raw platform string so a new build target never breaks the table.
DOCS_PLATFORM_LABELS = {
  "ruby" => "Source Archive",
  "x86_64-linux" => "Linux x86_64",
  "aarch64-linux" => "Linux arm64",
  "x86_64-linux-musl" => "Linux x86_64 (musl)",
  "aarch64-linux-musl" => "Linux arm64 (musl)",
  "x86_64-darwin" => "macOS x86_64",
  "arm64-darwin" => "macOS arm64",
  "x64-mingw-ucrt" => "Windows x64",
  "java" => "JRuby",
}.freeze
DOCS_PLATFORM_ORDER = DOCS_PLATFORM_LABELS.keys.freeze

# HEAD a URL and return whether it resolves to 2xx, following redirects. Retries a
# few times so a just-uploaded object has time to appear through CloudFront.
def url_reachable?(url, attempts:, wait: 5)
  attempts.times do |i|
    return true if head_ok?(url)

    sleep(wait) if i < attempts - 1
  end
  false
end

def head_ok?(url, redirects: 3)
  uri = URI(url)
  Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https",
                                      open_timeout: 10, read_timeout: 10) do |http|
    res = http.head(uri.request_uri)
    return head_ok?(res["location"], redirects: redirects - 1) if res.is_a?(Net::HTTPRedirection) && res["location"] && redirects.positive?
    return true if res.is_a?(Net::HTTPSuccess)

    # Some CDNs block HEAD (403/405) but serve GET — confirm with a 1-byte ranged
    # GET so we neither trust a false negative nor download a whole gem.
    res = http.get(uri.request_uri, "Range" => "bytes=0-0")
    return head_ok?(res["location"], redirects: redirects - 1) if res.is_a?(Net::HTTPRedirection) && res["location"] && redirects.positive?

    res.is_a?(Net::HTTPSuccess) # 200 or 206 Partial Content
  end
rescue StandardError
  false
end

# Print an AsciiDoc download table for the versioned artifacts (ready to paste into
# the release docs), then verify every link is reachable. Uses packages_base_url
# (the public docs domain); warns and falls back to an S3 URL if it is unset.
def print_download_table(gems, checksums, version, aws, dry_run)
  base = aws["packages_base_url"]
  if base.is_a?(String) && !base.empty?
    versioned = "#{base.chomp('/')}#{aws['packages_repo_path']}/sdk-#{version}"
  else
    versioned = "https://#{aws['packages_bucket']}.s3.amazonaws.com#{aws['packages_repo_path']}/sdk-#{version}"
    warn "  NOTE: aws.packages_base_url not set — table uses a best-effort S3 URL; set it to the docs domain."
  end

  # Checksums first, then gems ordered by platform (source archives first), name as
  # the tiebreak so the two source archives sort couchbase before -opentelemetry.
  gem_rows = gems.map do |g|
    plat = Gem::Package.new(g).spec.platform.to_s
    {label: DOCS_PLATFORM_LABELS[plat] || plat, file: File.basename(g), plat: plat, name: File.basename(g)}
  end
  gem_rows.sort_by! { |r| [DOCS_PLATFORM_ORDER.index(r[:plat]) || DOCS_PLATFORM_ORDER.size, r[:name]] }
  rows = [{label: "Checksums", file: File.basename(checksums)}] + gem_rows

  width = (["Platform"] + rows.map { |r| r[:label] }).map(&:length).max
  puts "\n== Download table (AsciiDoc — copy into the release docs)#{' [dry-run]' if dry_run} =="
  puts "| #{'Platform'.ljust(width)} | File"
  rows.each { |r| puts "| #{r[:label].ljust(width)} | #{versioned}/#{r[:file]}[#{r[:file]}]" }

  puts "\nVerifying links…"
  failures = rows.reject do |r|
    url = "#{versioned}/#{r[:file]}"
    ok = url_reachable?(url, attempts: dry_run ? 1 : 3)
    puts "  #{ok ? 'OK  ' : 'FAIL'} #{url}"
    ok
  end
  return if failures.empty?

  warn "\n  WARNING: #{failures.size} of #{rows.size} link(s) not reachable."
  warn "  If you just uploaded, CloudFront may need a minute to propagate — re-check shortly." unless dry_run
  warn "  (Expected in dry-run unless this version was already published.)" if dry_run
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Flush prints immediately so our prompts — and interactive subprocess prompts
# like gem push asking for an MFA OTP — always appear before input is read.
$stdout.sync = true

options = {
  execute: false,
  force: false,
  run_id: nil,
  work_dir: nil,
  skip: [],
  config: nil,
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{File.basename(__FILE__)} [options] <tag>"
  opts.on("--config[=FILE]", "Config file, or 'generate'/'print'/'edit' (bare --config prints)") do |v|
    options[:config] = v || "print"
  end
  opts.on("--execute", "Actually publish (default: dry-run)") { options[:execute] = true }
  opts.on("--force", "Allow overwriting an existing sdk-<version>/ S3 prefix") { options[:force] = true }
  opts.on("--run-id=ID", Integer, "Use this workflow run instead of resolving from the tag") { |v| options[:run_id] = v }
  opts.on("--work-dir=DIR", "Working directory (default: ./release-<tag>)") { |v| options[:work_dir] = v }
  opts.on("--skip=LIST", Array, "Comma list of stages to skip: rubygems,s3-version,s3-repo,docs") { |v| options[:skip] = v }
  opts.on("-h", "--help") do
    puts opts
    exit
  end
end
parser.parse!

case options[:config]
when "generate"
  generate_config(DEFAULT_CONFIG_PATH)
  exit
when "edit"
  edit_config(DEFAULT_CONFIG_PATH)
  exit
when "print"
  print_config(DEFAULT_CONFIG_PATH)
  exit
end

config_path = options[:config].nil? ? DEFAULT_CONFIG_PATH : options[:config]

tag = ARGV[0] or abort!("missing <tag>\n#{parser}")
runner = Runner.new(dry_run: !options[:execute])
config = load_config(config_path)

puts(options[:execute] ? "=== EXECUTE MODE — changes WILL be published ===" : "=== DRY-RUN (no changes; pass --execute to publish) ===")
puts "Tag: #{tag}"

# Preflight: the flow shells out to non-stdlib tools. Fail now with install hints
# rather than crashing halfway through a release. `aws` is only needed if at least
# one S3/docs stage will run.
require_command!("curl", "e.g. `sudo apt install curl`, `brew install curl`, `sudo dnf install curl`")
require_command!("unzip", "e.g. `sudo apt install unzip`, `brew install unzip`, `sudo dnf install unzip`")
require_command!("gpg", "e.g. `sudo apt install gnupg`, `brew install gnupg`, `sudo dnf install gnupg`")
unless (%w[s3-version s3-repo docs] - options[:skip]).empty?
  require_command!("aws", "AWS CLI v2 — https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html")
end

client = Octokit::Client.new(access_token: config.dig("github", "token"), auto_paginate: true)
repo = config.dig("github", "repo")
run_id = options[:run_id] || resolve_run_id(client, repo, tag, config.dig("github", "workflow"))

work_dir = File.expand_path(options[:work_dir] || "release-#{tag.delete_prefix('v')}")
zips = download_artifacts(client, repo, run_id, File.join(work_dir, "artifacts"))
gems, version = unpack_gems(zips, File.join(work_dir, "pkg"))
warn "NOTE: tag is #{tag} but the built gem version is #{version}" unless version == tag.delete_prefix('v')
puts "Gem version: #{version}"
puts "Final gems (#{gems.size}):"
gems.each { |g| puts "  #{File.basename(g)}" }
checksums = sign_checksums(gems, File.join(work_dir, "pkg"), version)

# RubyGems last: it is the public, irreversible step, so stage S3 first.
upload_versioned(gems, checksums, version, config["aws"], options[:force], runner) unless options[:skip].include?("s3-version")
update_gem_repo(gems, File.join(work_dir, "rubygems-repo"), config["aws"], runner) unless options[:skip].include?("s3-repo")

# After the versioned artifacts are up (and the repo invalidated), emit the docs
# download table and confirm every link resolves.
print_download_table(gems, checksums, version, config["aws"], runner.dry_run?) unless options[:skip].include?("s3-version")

unless options[:skip].include?("docs")
  docs_tree = prepare_docs(client, repo, run_id, version, work_dir)
  publish_docs(docs_tree, version, config["aws"], options[:force], runner)
end

unless options[:skip].include?("rubygems")
  # Show how to install from the freshly-deployed S3 repo, then push. push_to_rubygems
  # computes the plan, refuses on any checksum mismatch, and (in execute mode) gates
  # the irreversible push behind a typed version confirmation.
  print_verification(gems, version, config["aws"]) unless options[:skip].include?("s3-repo")
  push_to_rubygems(gems, version, runner)
end

puts "\nDone#{' (dry-run — nothing was published)' if runner.dry_run?}."
