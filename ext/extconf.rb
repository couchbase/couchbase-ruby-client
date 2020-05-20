require "mkmf"
require "fileutils"
require "tempfile"

unless find_executable("cmake")
  abort "ERROR: CMake is required to build couchbase extension."
end

def sys(*cmd)
  puts "-- #{Dir.pwd}"
  puts "-- #{cmd.join(" ")}"
  system(*cmd)
end

build_type = ENV["DEBUG"] ? "Debug" : "RelWithDebInfo"
project_path = File.expand_path(File.join(__dir__))
build_dir = File.join(Dir.tmpdir, "couchbase-rubygem-#{build_type}-#{RUBY_VERSION}-#{RUBY_PATCHLEVEL}-#{RUBY_PLATFORM}")
FileUtils.mkdir_p(build_dir)
Dir.chdir(build_dir) do
  sys("cmake",
      "-DCMAKE_BUILD_TYPE=#{build_type}",
      "-DTAOCPP_JSON_BUILD_TESTS=OFF",
      "-DTAOCPP_JSON_BUILD_EXAMPLES=OFF",
       project_path)
  sys("make -j4")
end
extension_path = File.expand_path(File.join(build_dir, 'libcouchbase.so'))
unless File.file?(extension_path)
  abort "ERROR: failed to build extension in #{extension_path}"
end
install_path = File.expand_path(File.join(__dir__, "..", "lib", "couchbase"))
puts "-- copy extension to #{install_path}"
FileUtils.cp(extension_path, install_path)
create_makefile("libcouchbase")