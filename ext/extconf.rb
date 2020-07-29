require "mkmf"
require "tempfile"

require_relative "../lib/couchbase/version"
SDK_VERSION = Couchbase::VERSION[:sdk]

unless find_executable("cmake")
  abort "ERROR: CMake is required to build couchbase extension."
end

def sys(*cmd)
  puts "-- #{Dir.pwd}"
  puts "-- #{cmd.join(" ")}"
  system(*cmd)
end


build_type = ENV["DEBUG"] ? "Debug" : "RelWithDebInfo"
cmake_flags = %W[
  -DCMAKE_BUILD_TYPE=#{build_type}
  -DRUBY_HDR_DIR=#{RbConfig::CONFIG["rubyhdrdir"]}
  -DRUBY_ARCH_HDR_DIR=#{RbConfig::CONFIG["rubyarchhdrdir"]}
  -DTAOCPP_JSON_BUILD_TESTS=OFF
  -DTAOCPP_JSON_BUILD_EXAMPLES=OFF
  -DSNAPPY_BUILD_TESTS=OFF
  -DSNAPPY_INSTALL=OFF
]

if ENV["CB_CC"]
  cmake_flags << "-DCMAKE_C_COMPILER=#{ENV["CB_CC"]}"
end
if ENV["CB_CXX"]
  cmake_flags << "-DCMAKE_CXX_COMPILER=#{ENV["CB_CXX"]}"
end
if ENV["CB_STATIC"]
  cmake_flags << "-DSTATIC_STDLIB=ON"
end

openssl_root = `brew --prefix openssl 2> /dev/null`.strip
unless openssl_root.empty?
  cmake_flags << "-DOPENSSL_ROOT_DIR=#{openssl_root}"
end

project_path = File.expand_path(File.join(__dir__))
build_dir = ENV['CB_EXT_BUILD_DIR'] || File.join(Dir.tmpdir, "cb-#{build_type}-#{RUBY_VERSION}-#{RUBY_PATCHLEVEL}-#{RUBY_PLATFORM}-#{SDK_VERSION}")
FileUtils.mkdir_p(build_dir)
Dir.chdir(build_dir) do
  puts "-- build #{build_type} extension #{SDK_VERSION} for ruby #{RUBY_VERSION}-#{RUBY_PATCHLEVEL}-#{RUBY_PLATFORM}"
  sys("cmake", *cmake_flags, project_path)
  sys("make -j4 VERBOSE=1")
end
extension_name = "libcouchbase.#{RbConfig::CONFIG["SOEXT"] || RbConfig::CONFIG["DLEXT"]}"
extension_path = File.expand_path(File.join(build_dir, extension_name))
unless File.file?(extension_path)
  abort "ERROR: failed to build extension in #{extension_path}"
end
extension_name.gsub!(/\.dylib/, '.bundle')
install_path = File.expand_path(File.join(__dir__, "..", "lib", "couchbase", extension_name))
puts "-- copy extension to #{install_path}"
FileUtils.cp(extension_path, install_path)
ext_directory = File.expand_path(__dir__)
if ENV["CB_REMOVE_EXT_DIRECTORY"]
  puts "-- CB_REMOVE_EXT_DIRECTORY is set, remove #{ext_directory}"
  Dir
      .glob("#{ext_directory}/*", File::FNM_DOTMATCH)
      .reject { |path| %w[. .. extconf.rb].include?(File.basename(path)) || File.basename(path).start_with?(".gem") }
      .each do |entry|
    puts "-- remove #{entry}"
    FileUtils.rm_rf(entry)
  end
  File.truncate("#{ext_directory}/extconf.rb", 0)
  puts "-- truncate #{ext_directory}/extconf.rb"
end
File.write("#{ext_directory}/Makefile", <<EOF)
.PHONY: all clean install
all:
clean:
install:
EOF
