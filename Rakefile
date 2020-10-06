#    Copyright 2020 Couchbase, Inc.
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

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/*_test.rb"]
end

desc "Compile binary extension"
task :compile do
  require 'tempfile'
  Dir.chdir(Dir.tmpdir) do
    sh "ruby #{File.join(__dir__, 'ext', 'extconf.rb')}"
  end
end

desc "Generate YARD documentation"
task :doc do
  require "couchbase/version"
  input_dir = File.join(__dir__, "lib")
  output_dir = File.join(__dir__, "doc", "couchbase-ruby-client-#{Couchbase::VERSION[:sdk]}")
  rm_rf output_dir
  sh "yard doc --hide-api private --output-dir #{output_dir} #{input_dir} --main README.md"
  puts "#{File.realpath(output_dir)}/index.html"
end

desc "An alias for documentation generation task"
task :docs => :doc

desc "Display stats on undocumented things"
task :undocumented => :doc do
  sh "yard stats --list-undoc --compact"
end

desc "Encode git revision into 'build_version.hxx.in' template (dependency of 'build' task)"
task :render_git_revision do
  build_version_path = File.join(__dir__, 'ext', 'build_version.hxx.in')
  File.write(build_version_path, File.read(build_version_path).gsub('@BACKEND_GIT_REVISION@', `git rev-parse HEAD`.strip))
end

desc "Build the package"
task :build => :render_git_revision
