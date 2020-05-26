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
B.open_bucket("default")
p B.upsert("default", "_default._default", "foo", '{"bar":42}')
p B.upsert("default", "_default._default", "bar", '{"bar":42}')
p B.upsert("default", "_default._default", "baz", '{"bar":42}')
)");

    run_script(R"(
B.close
)");

    ruby_finalize();
    return 0;
}
