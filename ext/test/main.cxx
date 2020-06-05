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
run_script(const char* script)
{
    spdlog::info("run script:\n----------------------------------------{}---------------------------------------\n", script);
    int status = 0;
    rb_eval_string_protect(script, &status);
    if (status != 0) {
        VALUE rbError = rb_funcall(rb_gv_get("$!"), rb_intern("message"), 0);
        spdlog::critical("ruby execution failure: {}", StringValuePtr(rbError));
        exit(EXIT_FAILURE);
    }
}

int
main()
{
    ruby_init();
    ruby_init_loadpath();

    rb_require(LIBCOUCHBASE_EXT_PATH);
    run_script(R"(
p Couchbase::VERSION
)");

    run_script(R"(
B = Couchbase::Backend.new
B.open("localhost", "Administrator", "password")
)");

       run_script(R"(
# p collection_create: (B.collection_create("default", "_default", "my_collection", nil, nil) rescue nil)
# p collection_create: (B.collection_create("default", "_default", "my_collection", nil, nil) rescue nil)
p open_bucket: B.open_bucket("default")
p document_get: B.document_get("default", "_default.my_collection", "bar", 15_000)
p document_upsert: B.document_upsert("default", "_default.my_collection", "foo", nil, "bar", 0, nil)
# p collection_drop: (B.collection_drop("default", "_default", "my_collection", nil) rescue nil)
# p collection_create: (B.collection_create("default", "_default", "my_collection", nil, nil) rescue nil)
start = Time.now
p document_get: (begin;B.document_get("default", "_default.my_collection", "foo", 15_000); rescue => ex; ex.message; end)
puts spent: Time.now - start
)");

    run_script(R"(
B.close
)");

    ruby_finalize();
    return 0;
}
