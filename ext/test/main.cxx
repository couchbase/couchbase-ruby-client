/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <generated_config.hxx>

#include <spdlog/spdlog.h>

#include <ruby.h>

void
run_script(const std::string & script)
{
    spdlog::info("run script:\n----------------------------------------{}---------------------------------------\n", script);
    int status = 0;
    rb_eval_string_protect(script.c_str(), &status);
    if (status != 0) {
        VALUE rbError = rb_funcall(rb_gv_get("$!"), rb_intern("message"), 0);
        spdlog::critical("ruby execution failure: {}", StringValuePtr(rbError));
        exit(EXIT_FAILURE);
    }
}

extern "C" {
void rb_encdb_declare(const char *name);
int rb_encdb_alias(const char *alias, const char *orig);
}

int
main(int argc, char **argv)
{
    ruby_sysinit(&argc, &argv);
    {
        RUBY_INIT_STACK;

        ruby_init();
        ruby_init_loadpath();

        rb_encdb_declare("ASCII-8BIT");
        rb_encdb_declare("US-ASCII");
        rb_encdb_declare("UTF-8");
        rb_encdb_alias("BINARY", "ASCII-8BIT");
        rb_encdb_alias("ASCII", "US-ASCII");


        rb_require(LIBCOUCHBASE_EXT_PATH);
        run_script(R"(
require "rubygems"
require "json"
p Couchbase::VERSION
include Couchbase

# begin
#   load "/home/avsej/code/couchbase-ruby-client/test/crud_test.rb"
# rescue => ex
#   p ex
#   puts ex.backtrace
#   rescue
# end

backend = Backend.new
connstr = "couchbase://localhost"
p open: backend.open(connstr, "Administrator", "password", {})
p bucket: backend.open_bucket("default", true)
p set: backend.document_upsert("default", "_default._default", "foo", 10_000, JSON.generate(foo: "bar"), 0, {})
p mutate: begin
backend.document_mutate_in(
    "default", "_default._default", "foo", 10_000,
    [
      {:opcode => :dict_add, :path => "foo1",  :param => "bar1"},
      {:opcode => :dict_add, :path => "foo2",  :param => "bar2"},
      {:opcode => :dict_add, :path => "foo3",  :param => "bar3"},
      {:opcode => :dict_add, :path => "foo4",  :param => "bar4"},
      {:opcode => :dict_add, :path => "foo5",  :param => "bar5"},
      {:opcode => :dict_add, :path => "foo6",  :param => "bar6"},
      {:opcode => :dict_add, :path => "foo7",  :param => "bar7"},
      {:opcode => :dict_add, :path => "foo8",  :param => "bar8"},
      {:opcode => :dict_add, :path => "foo9",  :param => "bar9"},
      {:opcode => :dict_add, :path => "foo10", :param => "bar10"},
      {:opcode => :dict_add, :path => "foo11", :param => "bar11"},
      {:opcode => :dict_add, :path => "foo12", :param => "bar12"},
      {:opcode => :dict_add, :path => "foo13", :param => "bar13"},
      {:opcode => :dict_add, :path => "foo14", :param => "bar14"},
      {:opcode => :dict_add, :path => "foo15", :param => "bar15"},
      {:opcode => :dict_add, :path => "foo16", :param => "bar16"},
      {:opcode => :dict_add, :path => "foo17", :param => "bar17"},
    ], {})
rescue => ex
ex
end
puts
sleep(1)
p close: backend.close

# backend = Backend.new
# begin
#   options = {
#   }
#   connstr = "couchbases://192.168.42.101?trust_certificate=/tmp/couchbase-ssl-certificate.pem"
#   # curl http://localhost:8091/pools/default/certificate > /tmp/couchbase-ssl-certificate.pem
#   connstr = "couchbases://localhost.avsej.net?trust_certificate=/tmp/couchbase-ssl-certificate.pem"
#   connstr = "couchbases://mars.local?trust_certificate=/tmp/couchbase-ssl-certificate.pem"
#   connstr = "couchbase://127.0.0.1:12000"
#   connstr = "couchbase://localhost"
#   p open: backend.open(connstr, "Administrator", "password", options)
# rescue => ex
#   p err: ex
#   puts ex.backtrace
# end
# p bucket: backend.open_bucket("default", true)
# keys = ('aaa'..'zzz').to_a.sample(10)
# keys.each do |key|
#     STDERR.puts("........ #{key} .........")
#     p set: backend.document_upsert("default", "_default._default", key, 10_000, JSON.generate(foo: "bar"), 0, {})
#     p get: (backend.document_get("default", "_default._default", key, nil) rescue nil)
# end
# (1..10000).to_a.reverse.each do |i|
#   sleep(1)
#   STDERR.puts("........ #{i} .........")
# end
# keys.each do |key|
#     STDERR.puts("........ #{key} .........")
#   #  p set: backend.document_upsert("default", "_default._default", key, 10_000, JSON.generate(foo: "bar"), 0, {})
#     p get: (backend.document_get("default", "_default._default", key, nil) rescue nil)
# end
# p query: backend.document_query('select "ruby rules" as greeting', {})
# STDERR.puts("prepare to close backend")
# p close: backend.close

# backend = Backend.new
# begin
#   p open: backend.open("couchbase://localhost", "Administrator", "password")
# rescue => ex
#   p err: ex
#   puts ex.backtrace
# end
# backend.close

# 100.times do |idx|
#   puts "------ #{idx} -----"
#   backend = Backend.new
#   p open: backend.open("couchbase://192.168.42.101", "Administrator", "password")
#   p bucket: backend.open_bucket("default", false)
#   p set: backend.document_upsert("default", "_default._default", "hello", 10_000, JSON.generate(foo: "bar"), 0, {})
#   p get: backend.document_get("default", "_default._default", "hello", nil)
#   p close: backend.close
# end

# backend = Backend.new
# p open: backend.open("couchbase://localhost", "Administrator", "password")
# p query1: (begin; backend.document_query('select curl("https://rubygems.org/api/v1/versions/couchbase.json")', {timeout: 100})[:meta]; rescue => ex; ex; end)
# p query2: backend.document_query('select curl("https://rubygems.org/api/v1/versions/couchbase.json")', {})[:meta]
# p query3: backend.document_query('select curl("https://rubygems.org/api/v1/versions/couchbase.json")', {})[:meta]
# p close: backend.close

# 100.times do
#   %w[query crud subdoc].each do |suite|
#     begin
#       load "/home/avsej/code/couchbase-ruby-client/test/#{suite}_test.rb"
#     rescue => ex
#       p ex
#       puts ex.backtrace
#     end
#   end
# end
)");

        ruby_finalize();
    }
    return 0;
}
