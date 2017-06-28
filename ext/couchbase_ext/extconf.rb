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
    cfg['CFLAGS'].gsub!(RbConfig::ARCHFLAGS, '')
    cfg['LDFLAGS'].gsub!(RbConfig::ARCHFLAGS, '')
    cfg['LDSHARED'].gsub!(RbConfig::ARCHFLAGS, '')
    cfg['LIBRUBY_LDSHARED'].gsub!(RbConfig::ARCHFLAGS, '')
    cfg['configure_args'].gsub!(RbConfig::ARCHFLAGS, '')
  end
end

# Unset RUBYOPT to avoid interferences
ENV['RUBYOPT'] = nil

require 'mkmf'

def define(macro, value = nil)
  $defs.push("-D #{[macro.upcase, value].compact.join('=')}")
end

($CFLAGS  ||= '') << " #{ENV['CFLAGS']}"
($LDFLAGS ||= '') << " #{ENV['LDFLAGS']}"
($LIBS    ||= '') << " #{ENV['LIBS']}"

if RbConfig::CONFIG['target_os'] =~ /mingw32/
  CONFIG['CC'] = CONFIG['CXX']
  $LDFLAGS << ' -static-libgcc -static-libstdc++ -Wl,--strip-debug'
  $CFLAGS << ' -fpermissive -std=c++11'
  dir_config('libcouchbase')
else
  $CFLAGS << ' -std=c99 -Wall -Wextra '
  if ENV['DEBUG']
    $CFLAGS << ' -O0 -ggdb3 -pedantic '
  else
    $CFLAGS << ' -O2'
  end
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
  brew_prefix = `brew --prefix libcouchbase 2> /dev/null`.chomp
  unless brew_prefix.empty?
    LIB_DIRS.unshift File.join(brew_prefix, 'lib')
    HEADER_DIRS.unshift File.join(brew_prefix, 'include')
  end

  HEADER_DIRS.delete_if { |d| !File.exist?(d) }
  LIB_DIRS.delete_if { |d| !File.exist?(d) }

  # it will find the libcouchbase likely. you can specify its path otherwise
  #
  #   ruby extconf.rb [--with-libcouchbase-include=<dir>] [--with-libcouchbase-lib=<dir>]
  #
  # or
  #
  #   ruby extconf.rb [--with-libcouchbase-dir=<dir>]
  #
  dir_config('libcouchbase', HEADER_DIRS, LIB_DIRS)
end

if COMMON_HEADERS !~ /"ruby\.h"/
  (COMMON_HEADERS ||= '') << %(\n#include "ruby.h"\n)
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
  define('HAVE_STDARG_PROTOTYPES')
end

def die(message)
  STDERR.puts "\n#{'*' * 70}"
  STDERR.puts "#{message.gsub(/^/, '* ')}"
  STDERR.puts "#{'*' * 70}\n\n"
  abort
end

minimal_lcb_version = '2.9.2'
install_notice = "You must install libcouchbase >= #{minimal_lcb_version}
See http://developer.couchbase.com/server/other-products/release-notes-archives/c-sdk for more details"

unless try_compile(<<-SRC)
  #include <libcouchbase/couchbase.h>
  #include <stdio.h>

  int main() {
#if LCB_VERSION < 0x#{minimal_lcb_version.split('.').map { |x| format('%02x', x.to_i) }.join}
#  error "libcouchbase is too old"
#endif
    return 0;
  }
  SRC
  die(install_notice)
end

unless 'foo()'.respond_to?(:funcall_style)
  class String
    def funcall_style
      /\)\z/ =~ self ? dup : "#{self}()"
    end
  end

  def try_func(func, libs, headers = nil, &b)
    headers = cpp_include(headers)
    try_link(<<"SRC", libs, &b) or try_link(<<"SRC", libs, &b)
#{COMMON_HEADERS}
#{headers}
/*top*/
int main() { return 0; }
int t() { void ((*volatile p)()); p = (void ((*)()))#{func}; return 0; }
SRC
#{headers}
/*top*/
int main() { return 0; }
int t() { #{func.funcall_style}; return 0; }
SRC
  end
end

# just to add -lcouchbase properly
have_library('couchbase', 'lcb_iops_wire_bsd_impl2(NULL, 0)', 'libcouchbase/couchbase.h') || die(install_notice)
have_header('mach/mach_time.h')
have_header('stdint.h') || die('Failed to locate stdint.h')
have_header('sys/time.h') unless RbConfig::CONFIG['target_os'] =~ /mingw32/
have_header('fcntl.h')
have_header('sys/socket.h')
have_header('errno.h')

have_type('st_index_t')
have_func('clock_gettime')
have_func('gettimeofday')
have_func('QueryPerformanceCounter')
have_func('gethrtime')
have_func('rb_hash_lookup2')
have_func('rb_thread_fd_select')
have_func('rb_thread_blocking_region')
have_func('rb_thread_call_without_gvl')
have_func('poll', 'poll.h')
have_func('ppoll', 'poll.h')
have_func('rb_fiber_yield')
define('_GNU_SOURCE')
create_header('couchbase_config.h')
create_makefile('couchbase_ext')
