#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "open3"
require "rbconfig"
require "tmpdir"
require "rubygems/installer"
require "rubygems/package"

module Gem
  class Collector
    def initialize(gemfiles)
      @gemfiles = gemfiles
    end

    def bundle(target_dir)
      installers = []

      first_gemspec = nil
      known_abis = []
      @gemfiles.each do |gemfile|
        installer = prepare_installer(gemfile)
        first_gemspec ||= installer.spec.dup # collect everything to first gemspec
        installers << installer

        puts "Unpacking gem in temporary directory '#{installer.spec.full_gem_path}'..."
        installer.package.extract_files(installer.spec.full_gem_path)

        # determine build artifacts from require_paths
        dlext = RbConfig::CONFIG["DLEXT"]
        lib_dirs = installer.spec.require_paths.join(",")
        abi = installer.spec.required_ruby_version.requirements.flatten.last.segments[0, 2].join(".")
        known_abis |= [abi]
        Dir.glob("#{installer.spec.full_gem_path}/{#{lib_dirs}}/**/*.#{dlext}").map do |path|
          dirname, basename = File.split(path)
          new_dirname = File.join(dirname, abi)
          new_path = File.join(new_dirname, basename).gsub(installer.spec.full_gem_path, first_gemspec.full_gem_path)
          FileUtils.mkdir_p(File.dirname(new_path))
          FileUtils.mv(path, new_path)
          system("strip", "--strip-all", new_path) || system("strip", new_path)
          file = new_path.sub("#{first_gemspec.full_gem_path}/", "")
          puts "Adding '#{file}' to gemspec"
          first_gemspec.files.push file
        end
      end

      if first_gemspec
        Dir.chdir(first_gemspec.full_gem_path) do
          File.write("lib/couchbase/libcouchbase.rb", <<-RUBY)
        begin
          require_relative "\#{RUBY_VERSION[/(\\d+\\.\\d+)/]}/libcouchbase"
        rescue LoadError
          raise LoadError, "unable to load couchbase extension for Ruby \#{RUBY_VERSION}. Only available for #{known_abis.join(', ')}. " \\
            "Try to install couchbase from sources with 'gem install --platform ruby couchbase'"
        end
          RUBY
          first_gemspec.files.push "lib/couchbase/libcouchbase.rb"
        end
        # remove any non-existing files
        first_gemspec.files.select! { |f| File.exist?(File.join(first_gemspec.full_gem_path, f)) }

        result = repackage(first_gemspec)
        output = File.join(target_dir, File.basename(result))
        FileUtils.mv(result, output)
        puts "Repackaged library as: #{output}"
      else
        puts "No gems found"
      end

      installers.each do |installer|
        FileUtils.rm_rf File.dirname(installer.spec.full_gem_path)
      end
    end

    def prepare_installer(gemfile)
      basename = File.basename(gemfile, ".gem")
      installer = Gem::Installer.at(gemfile, {unpack: true})
      installer.spec.full_gem_path = File.join(Dir.glob(Dir.mktmpdir).first, basename)
      installer.spec.extension_dir = File.join(installer.spec.full_gem_path, "lib")
      installer
    end

    def repackage(gemspec)
      # clear out extensions from gemspec
      gemspec.extensions.clear

      # adjust platform
      gemspec.platform = Gem::Platform::CURRENT
      gemspec.required_ruby_version = "> 3.0"

      # build new gem
      output_gem = nil

      Dir.chdir gemspec.full_gem_path do
        output_gem = Gem::Package.build(gemspec)
      end

      abort "There was a problem building the gem." unless output_gem
      File.join(gemspec.full_gem_path, output_gem)
    end
  end
end

pkg_dir = File.expand_path("../../pkg", __dir__)
gemfiles = Dir[File.join(pkg_dir, "binary/**/*.gem")]
collector = Gem::Collector.new(gemfiles)
output_dir = File.join(pkg_dir, "fat")
FileUtils.rm_rf(output_dir)
FileUtils.mkdir_p(output_dir)
collector.bundle(output_dir)
