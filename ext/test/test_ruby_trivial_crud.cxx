/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "test_helper_ruby.hxx"

TEST_CASE("ruby: upsert document into default collection", "[ruby]")
{
    TEST_PREAMBLE_RUBY;
    auto ctx = test_context::load_from_environment();

    auto error = ruby.eval_script(ctx, R"(
backend = Couchbase::Backend.new
backend.open(CONNECTION_STRING, {username: USERNAME, password: PASSWORD}, {})
backend.open_bucket(BUCKET, true)
backend.document_upsert(BUCKET, "_default._default", "foo", JSON.generate(foo: "bar"), 0, {})
backend.close
)");
    if (error) {
        FAIL(error.value());
    }
}
