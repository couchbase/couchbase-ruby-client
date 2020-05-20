require "mkmf"
require "fileutils"
require "tempfile"
require "rbconfig"

unless find_executable("cmake")
  abort "ERROR: CMake is required to build couchbase extension."
end

def sys(*cmd)
  puts "-- #{Dir.pwd}"
  puts "-- #{cmd.join(" ")}"
  system(*cmd)
end

build_type = ENV["DEBUG"] ? "Debug" : "RelWithDebInfo"
cmake_flags = [
  "-DCMAKE_BUILD_TYPE=#{build_type}",
  "-DRUBY_HDR_DIR=#{RbConfig::CONFIG["rubyhdrdir"]}",
  "-DRUBY_ARCH_HDR_DIR=#{RbConfig::CONFIG["rubyarchhdrdir"]}",
  "-DTAOCPP_JSON_BUILD_TESTS=OFF",
  "-DTAOCPP_JSON_BUILD_EXAMPLES=OFF",
]
openssl_root = `brew --prefix openssl 2> /dev/null`.strip
if openssl_root
  cmake_flags << "-DOPENSSL_ROOT_DIR=#{openssl_root}"
end

project_path = File.expand_path(File.join(__dir__))
build_dir = File.join(Dir.tmpdir, "couchbase-rubygem-#{build_type}-#{RUBY_VERSION}-#{RUBY_PATCHLEVEL}-#{RUBY_PLATFORM}")
FileUtils.mkdir_p(build_dir)
Dir.chdir(build_dir) do
  sys("cmake", *cmake_flags, project_path)
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
