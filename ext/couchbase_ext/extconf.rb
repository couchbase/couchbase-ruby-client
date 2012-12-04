# encoding: UTF-8
# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011, 2012 Couchbase, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'rbconfig'
# This hack is more robust, because in bundler environment bundler touches
# all constants from rbconfig.rb before loading any scripts. This is why
# RC_ARCHS doesn't work under bundler on MacOS.
if RUBY_PLATFORM =~ /darwin/ && defined?(RbConfig::ARCHFLAGS)
  [RbConfig::CONFIG, RbConfig::MAKEFILE_CONFIG].each do |cfg|
    cfg["CFLAGS"].gsub!(RbConfig::ARCHFLAGS, '')
    cfg["LDFLAGS"].gsub!(RbConfig::ARCHFLAGS, '')
    cfg["LDSHARED"].gsub!(RbConfig::ARCHFLAGS, '')
    cfg["LIBRUBY_LDSHARED"].gsub!(RbConfig::ARCHFLAGS, '')
    cfg["configure_args"].gsub!(RbConfig::ARCHFLAGS, '')
  end
end

# Unset RUBYOPT to avoid interferences
ENV['RUBYOPT'] = nil

require 'mkmf'

def define(macro, value = nil)
  $defs.push("-D #{[macro.upcase, value].compact.join('=')}")
end

($CFLAGS  ||= "") << " #{ENV["CFLAGS"]}"
($LDFLAGS ||= "") << " #{ENV["LDFLAGS"]}"
($LIBS    ||= "") << " #{ENV["LIBS"]}"

$CFLAGS << ' -std=c99 -Wall -Wextra '
if ENV['DEBUG']
  $CFLAGS << ' -O0 -ggdb3 -pedantic '
end

if RbConfig::CONFIG['target_os'] =~ /mingw32/
  dir_config("libcouchbase")
else
  LIBDIR = RbConfig::CONFIG['libdir']
  INCLUDEDIR = RbConfig::CONFIG['includedir']

  HEADER_DIRS = [
    # First search /opt/local for macports
    '/opt/local/include',
    # Then search /usr/local for people that installed from source
    '/usr/local/include',
    # Check the ruby install locations
    INCLUDEDIR,
    # Finally fall back to /usr
    '/usr/include'
  ]

  LIB_DIRS = [
    # First search /opt/local for macports
    '/opt/local/lib',
    # Then search /usr/local for people that installed from source
    '/usr/local/lib',
    # Check the ruby install locations
    LIBDIR,
    # Finally fall back to /usr
    '/usr/lib'
  ]

  # For people using homebrew
  brew_prefix = `brew --prefix libevent 2> /dev/null`.chomp
  unless brew_prefix.empty?
    LIB_DIRS.unshift File.join(brew_prefix, 'lib')
    HEADER_DIRS.unshift File.join(brew_prefix, 'include')
  end

  HEADER_DIRS.delete_if{|d| !File.exists?(d)}
  LIB_DIRS.delete_if{|d| !File.exists?(d)}

  # it will find the libcouchbase likely. you can specify its path otherwise
  #
  #   ruby extconf.rb [--with-libcouchbase-include=<dir>] [--with-libcouchbase-lib=<dir>]
  #
  # or
  #
  #   ruby extconf.rb [--with-libcouchbase-dir=<dir>]
  #
  dir_config("libcouchbase", HEADER_DIRS, LIB_DIRS)
end

if COMMON_HEADERS !~ /"ruby\.h"/
  (COMMON_HEADERS ||= "") << %(\n#include "ruby.h"\n)
end

if try_compile(<<-SRC)
  #include <stdarg.h>
  int foo(int x, ...) {
    va_list va;
    va_start(va, x);
    va_arg(va, int);
    va_arg(va, char *);
    va_arg(va, double);
    return 0;
  }
  int main() {
    return foo(10, "", 3.14);
    return 0;
  }
  SRC
  define("HAVE_STDARG_PROTOTYPES")
end

def die(message)
  STDERR.puts "\n#{"*" * 70}"
  STDERR.puts "#{message}"
  STDERR.puts "#{"*" * 70}\n\n"
  abort
end

have_library("couchbase", "lcb_verify_compiler_setup", "libcouchbase/couchbase.h") or die("You must install libcouchbase >= 2.0.0beta3. See http://www.couchbase.com/develop/ for more details")
have_header("mach/mach_time.h")
have_header("stdint.h") or die("Failed to locate stdint.h")
have_header("sys/time.h")
have_func("clock_gettime")
have_func("gettimeofday")
have_func("QueryPerformanceCounter")
have_func("rb_hash_lookup2")
define("_GNU_SOURCE")
create_header("couchbase_config.h")
create_makefile("couchbase_ext")
