# frozen_string_literal: true

#  Copyright 2022-Present Couchbase, Inc.
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

require "tmpdir"

def find_executable(cmd)
  exts = RbConfig::CONFIG["EXECUTABLE_EXTS"].split | [RbConfig::CONFIG["EXEEXT"]]
  ENV["PATH"].split(File::PATH_SEPARATOR).each do |path|
    path.strip!
    next if path.empty?

    path = File.join(path, cmd)
    exts.each do |ext|
      executable_path = path + ext
      if File.executable?(executable_path)
        puts("# found #{cmd}: #{executable_path}")
        return executable_path
      end
    end
  end
  abort "#{cmd}: command not found"
end

require "open-uri"

desc "Re-generate gRPC client implementation"
task :generate_grpc_client do
  protoc_binary = find_executable("grpc_tools_ruby_protoc")
  root_dir = File.expand_path("..", __dir__)
  stellar_nebula_dir = File.expand_path("deps/stellar-nebula", root_dir)
  proto_files = Dir["#{stellar_nebula_dir}/proto/**/*.proto"]
  lib_dir = File.expand_path("lib", root_dir)
  output_dir = File.join(lib_dir, "couchbase/stellar_nebula/generated")
  Dir.mktmpdir do |tmpdir|
    sh(protoc_binary,
       "--proto_path=#{stellar_nebula_dir.shellescape}/proto",
       "--proto_path=#{stellar_nebula_dir.shellescape}/contrib/googleapis",
       "--grpc_out=#{tmpdir.shellescape}",
       "--ruby_out=#{tmpdir.shellescape}",
       *proto_files)
    Dir["#{tmpdir}/**/*.rb"].each do |file|
      content = File.read(file)
                    .gsub(/^require (['"])couchbase\/(\w+?\/)?/, 'require_relative \1')
      File.write(file, content)
    end
    rm_rf(output_dir)
    mv("#{tmpdir}/couchbase", output_dir)
  end
end

desc "Re-generate gRPC client and reformat using Rubocop"
task generate: %w[generate_grpc_client rubocop:autocorrect_all]
