#  Copyright 2020-2021 Couchbase, Inc.
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

require "bundler/gem_tasks"
require "rake/testtask"
require "rubocop/rake_task"

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/*_test.rb"]
end

desc "Compile binary extension"
task :compile do
  require 'tempfile'
  Dir.chdir(Dir.tmpdir) do
    sh "ruby '#{File.join(__dir__, 'ext', 'extconf.rb')}'"
  end
end

desc "Generate YARD documentation"
task :doc do
  require "couchbase/version"
  input_dir = File.join(__dir__, "lib")
  output_dir = File.join(__dir__, "doc", "couchbase-ruby-client-#{Couchbase::VERSION[:sdk]}")
  rm_rf output_dir
  sh "bundle exec yard doc --no-progress --hide-api private --output-dir #{output_dir} #{input_dir} --main README.md"
  puts "#{File.realpath(output_dir)}/index.html"
end

desc "An alias for documentation generation task"
task :docs => :doc

desc "Display stats on undocumented things"
task :undocumented => :doc do
  sh "yard stats --list-undoc --compact"
end

desc "Encode git revision into 'ext/extconf.rb' template (dependency of 'build' task)"
task :render_git_revision do
  library_revision = Dir.chdir(__dir__) { `git rev-parse HEAD`.strip }
  core_revision = Dir.chdir(File.join(__dir__, 'ext', 'couchbase')) { `git rev-parse HEAD`.strip }
  core_describe = Dir.chdir(File.join(__dir__, 'ext', 'couchbase')) do
    `git fetch --tags >/dev/null 2>&1`
    `git describe --long --always HEAD`.strip
  end
  File.open(File.join(__dir__, "ext", "cache", "extconf_include.rb"), "a+") do |io|
    io.puts(<<~REVISIONS)
      cmake_flags << "-DEXT_GIT_REVISION=#{library_revision}"
      cmake_flags << "-DCOUCHBASE_CXX_CLIENT_GIT_REVISION=#{core_revision}"
      cmake_flags << "-DCOUCHBASE_CXX_CLIENT_GIT_DESCRIBE=#{core_describe}"
    REVISIONS
  end
end

def which(name, extra_locations = [])
  ENV.fetch("PATH", "")
     .split(File::PATH_SEPARATOR)
     .prepend(*extra_locations)
     .select { |path| File.directory?(path) }
     .map { |path| [path, name].join(File::SEPARATOR) + RbConfig::CONFIG["EXEEXT"] }
     .find { |file| File.executable?(file) }
end

desc "Download and cache dependencies of C++ core"
task :cache_cxx_dependencies do
  require "tempfile"
  require "rubygems/package"

  output_dir = Dir.mktmpdir("cxx_output_")
  output_tarball = File.join(output_dir, "cache.tar")
  cpm_cache_dir = Dir.mktmpdir("cxx_cache_")
  cxx_core_build_dir =  Dir.mktmpdir("cxx_build_")
  cxx_core_source_dir = File.join(__dir__, "ext", "couchbase")
  cc = ENV.fetch("CB_CC", nil)
  cxx = ENV.fetch("CB_CXX", nil)
  ar = ENV.fetch("CB_AR", nil)

  cmake_extra_locations = []
  if RUBY_PLATFORM.match?(/mswin|mingw/)
    cmake_extra_locations = [
      'C:\Program Files\CMake\bin',
      'C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin',
      'C:\Program Files\Microsoft Visual Studio\2019\Professional\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin',
    ]
    local_app_data = ENV.fetch("LOCALAPPDATA", "#{Dir.home}\\AppData\\Local")
    cmake_extra_locations.unshift("#{local_app_data}\\CMake\\bin") if File.directory?(local_app_data)
    cc = RbConfig::CONFIG["CC"]
    cxx = RbConfig::CONFIG["CXX"]
  end
  cmake = which("cmake", cmake_extra_locations) || which("cmake3", cmake_extra_locations)
  cmake_flags = [
    "-S#{cxx_core_source_dir}",
    "-B#{cxx_core_build_dir}",
    "-DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=OFF",
    "-DCOUCHBASE_CXX_CLIENT_BUILD_TOOLS=OFF",
    "-DCOUCHBASE_CXX_CLIENT_BUILD_DOCS=OFF",
    "-DCOUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON",
    "-DCPM_DOWNLOAD_ALL=ON",
    "-DCPM_USE_NAMED_CACHE_DIRECTORIES=ON",
    "-DCPM_USE_LOCAL_PACKAGES=OFF",
    "-DCPM_SOURCE_CACHE=#{cpm_cache_dir}",
  ]
  cmake_flags << "-DCMAKE_C_COMPILER=#{cc}" if cc
  cmake_flags << "-DCMAKE_CXX_COMPILER=#{cxx}" if cxx
  cmake_flags << "-DCMAKE_AR=#{ar}" if ar

  puts("-----> run cmake to dowload all depenencies (#{cmake})")
  sh(cmake, *cmake_flags, verbose: true)

  puts("-----> create archive with whitelisted sources: #{output_tarball}")
  File.open(output_tarball, "w+b") do |file|
    Gem::Package::TarWriter.new(file) do |writer|
      chdir(cxx_core_build_dir, verbose: true) do
        ["mozilla-ca-bundle.sha256", "mozilla-ca-bundle.crt"].each do |path|
          writer.add_file(path, 0o660) { |io| io.write(File.binread(path)) }
        end
      end
      chdir(cpm_cache_dir, verbose: true) do
        third_party_sources = Dir[
          "cpm/*.cmake",
          "asio/*/LICENSE*",
          "asio/*/asio/COPYING",
          "asio/*/asio/asio/include/*.hpp",
          "asio/*/asio/asio/include/asio/**/*.[hi]pp",
          "boringssl/*/boringssl/**/*.{cc,h,c,asm,S}",
          "boringssl/*/boringssl/**/CMakeLists.txt",
          "boringssl/*/boringssl/LICENSE",
          "fmt/*/fmt/CMakeLists.txt",
          "fmt/*/fmt/ChangeLog.rst",
          "fmt/*/fmt/LICENSE.rst",
          "fmt/*/fmt/README.rst",
          "fmt/*/fmt/include/**/*",
          "fmt/*/fmt/src/**/*",
          "fmt/*/fmt/support/cmake/**/*",
          "gsl/*/gsl/CMakeLists.txt",
          "gsl/*/gsl/GSL.natvis",
          "gsl/*/gsl/LICENSE*",
          "gsl/*/gsl/ThirdPartyNotices.txt",
          "gsl/*/gsl/cmake/*",
          "gsl/*/gsl/include/**/*",
          "hdr_histogram/*/hdr_histogram/*.pc.in",
          "hdr_histogram/*/hdr_histogram/CMakeLists.txt",
          "hdr_histogram/*/hdr_histogram/COPYING.txt",
          "hdr_histogram/*/hdr_histogram/LICENSE.txt",
          "hdr_histogram/*/hdr_histogram/cmake/*",
          "hdr_histogram/*/hdr_histogram/config.cmake.in",
          "hdr_histogram/*/hdr_histogram/include/**/*",
          "hdr_histogram/*/hdr_histogram/src/**/*",
          "json/*/json/CMakeLists.txt",
          "json/*/json/LICENSE*",
          "json/*/json/external/PEGTL/.cmake/*",
          "json/*/json/external/PEGTL/CMakeLists.txt",
          "json/*/json/external/PEGTL/LICENSE*",
          "json/*/json/external/PEGTL/include/**/*",
          "json/*/json/include/**/*",
          "llhttp/*/llhttp/*.pc.in",
          "llhttp/*/llhttp/CMakeLists.txt",
          "llhttp/*/llhttp/LICENSE*",
          "llhttp/*/llhttp/include/*.h",
          "llhttp/*/llhttp/src/*.c",
          "snappy/*/snappy/CMakeLists.txt",
          "snappy/*/snappy/COPYING",
          "snappy/*/snappy/cmake/*",
          "snappy/*/snappy/snappy-c.{h,cc}",
          "snappy/*/snappy/snappy-internal.h",
          "snappy/*/snappy/snappy-sinksource.{h,cc}",
          "snappy/*/snappy/snappy-stubs-internal.{h,cc}",
          "snappy/*/snappy/snappy-stubs-public.h.in",
          "snappy/*/snappy/snappy.{h,cc}",
          "spdlog/*/spdlog/CMakeLists.txt",
          "spdlog/*/spdlog/LICENSE",
          "spdlog/*/spdlog/cmake/*",
          "spdlog/*/spdlog/include/**/*",
          "spdlog/*/spdlog/src/**/*",
        ].grep_v(/crypto_test_data.cc/)

        # we don't want to fail if git is not available
        cpm_cmake_path = third_party_sources.grep(/cpm.*\.cmake$/).first
        File.write(cpm_cmake_path, File.read(cpm_cmake_path).gsub("Git REQUIRED", "Git"))

        third_party_sources
          .select { |path| File.file?(path) }
          .each { |path| writer.add_file(path, 0o660) { |io| io.write(File.binread(path)) } }
      end
    end
  end

  rm_rf(cxx_core_build_dir, verbose: true)
  rm_rf(cpm_cache_dir, verbose: true)

  untar = ["tar", "-x"]
  untar << "--force-local" unless RUBY_PLATFORM.match?(/darwin/)

  puts("-----> verify that tarball works as a cache for CPM")
  cxx_core_build_dir = Dir.mktmpdir("cxx_build_")
  cpm_cache_dir = Dir.mktmpdir("cxx_cache_")
  chdir(cpm_cache_dir, verbose: true) do
    sh(*untar, "-f", output_tarball, verbose: true)
  end

  cmake_flags = [
    "-S#{cxx_core_source_dir}",
    "-B#{cxx_core_build_dir}",
    "-DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=OFF",
    "-DCOUCHBASE_CXX_CLIENT_BUILD_TOOLS=OFF",
    "-DCOUCHBASE_CXX_CLIENT_BUILD_DOCS=OFF",
    "-DCOUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON",
    "-DCPM_DOWNLOAD_ALL=OFF",
    "-DCPM_USE_NAMED_CACHE_DIRECTORIES=ON",
    "-DCPM_USE_LOCAL_PACKAGES=OFF",
    "-DCPM_SOURCE_CACHE=#{cpm_cache_dir}",
    "-DCOUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT=#{cpm_cache_dir}",
  ]
  cmake_flags << "-DCMAKE_C_COMPILER=#{cc}" if cc
  cmake_flags << "-DCMAKE_CXX_COMPILER=#{cxx}" if cxx
  cmake_flags << "-DCMAKE_AR=#{ar}" if ar

  sh(cmake, *cmake_flags, verbose: true)

  rm_rf(cxx_core_build_dir, verbose: true)
  rm_rf(cpm_cache_dir, verbose: true)

  cache_dir = File.join(__dir__, "ext", "cache")
  rm_rf(cache_dir, verbose: true)
  abort("unable to remove #{cache_dir}") if File.directory?(cache_dir)
  mkdir_p(cache_dir, verbose: true)
  chdir(cache_dir, verbose: true) do
    sh(*untar, "-f", output_tarball, verbose: true)
  end
  rm_rf(output_dir, verbose: true)

  extconf_include = File.join(cache_dir, "extconf_include.rb")
  File.open(extconf_include, "w+") do |io|
    io.puts(<<~CACHE_FLAGS)
      cmake_flags << "-DCPM_DOWNLOAD_ALL=OFF"
      cmake_flags << "-DCPM_USE_NAMED_CACHE_DIRECTORIES=ON"
      cmake_flags << "-DCPM_USE_LOCAL_PACKAGES=OFF"
      cmake_flags << "-DCPM_SOURCE_CACHE=\#{File.expand_path('cache', __dir__)}"
      cmake_flags << "-DCOUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE_ROOT=\#{File.expand_path('cache', __dir__)}"
    CACHE_FLAGS
  end
end

desc "Build the package"
task :build => [:cache_cxx_dependencies, :render_git_revision]

RuboCop::RakeTask.new

load "task/grpc.rake"
