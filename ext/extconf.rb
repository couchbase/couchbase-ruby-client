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
cmake_flags = [
  "-DCMAKE_BUILD_TYPE=#{build_type}",
  "-DRUBY_HDR_DIR=#{RbConfig::CONFIG["rubyhdrdir"]}",
  "-DRUBY_ARCH_HDR_DIR=#{RbConfig::CONFIG["rubyarchhdrdir"]}",
  "-DTAOCPP_JSON_BUILD_TESTS=OFF",
  "-DTAOCPP_JSON_BUILD_EXAMPLES=OFF",
  "-DSNAPPY_BUILD_TESTS=OFF",
  "-DSNAPPY_INSTALL=OFF",
]
openssl_root = `brew --prefix openssl 2> /dev/null`.strip
unless openssl_root.empty?
  cmake_flags << "-DOPENSSL_ROOT_DIR=#{openssl_root}"
end

project_path = File.expand_path(File.join(__dir__))
build_dir = ENV['EXT_BUILD_DIR'] || File.join(Dir.tmpdir, "couchbase-rubygem-#{build_type}-#{RUBY_VERSION}-#{RUBY_PATCHLEVEL}-#{RUBY_PLATFORM}")
FileUtils.mkdir_p(build_dir)
Dir.chdir(build_dir) do
  sys("cmake", *cmake_flags, project_path)
  sys("make -j4 VERBOSE=1")
end
extension_name = "libcouchbase.#{RbConfig::CONFIG["SOEXT"]}"
extension_path = File.expand_path(File.join(build_dir, extension_name))
unless File.file?(extension_path)
  abort "ERROR: failed to build extension in #{extension_path}"
end
extension_name.gsub!(/\.dylib/, '.bundle')
install_path = File.expand_path(File.join(__dir__, "..", "lib", "couchbase", extension_name))
puts "-- copy extension to #{install_path}"
FileUtils.cp(extension_path, install_path)
create_makefile("libcouchbase")
