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

#pragma once

#include "test_helper.hxx"
#include <optional>

#include <ruby.h>

extern "C" {
void
rb_encdb_declare(const char* name);
int
rb_encdb_alias(const char* alias, const char* orig);
}

struct ruby_context {
    ruby_context()
    {
        ruby_init();
        ruby_init_loadpath();

        rb_encdb_declare("ASCII-8BIT");
        rb_encdb_declare("US-ASCII");
        rb_encdb_declare("UTF-8");
        rb_encdb_alias("BINARY", "ASCII-8BIT");
        rb_encdb_alias("ASCII", "US-ASCII");
        rb_require("rubygems");
        rb_require("json");
        rb_require(LIBCOUCHBASE_EXT_PATH);
    }

    ~ruby_context()
    {
        ruby_cleanup(0);
    }

    std::optional<std::string> eval_script(test_context& ctx, const std::string& input)
    {
        int status = 0;
        std::string script = input;
        script = std::regex_replace(script, std::regex("CONNECTION_STRING"), '"' + ctx.connection_string + '"');
        script = std::regex_replace(script, std::regex("USERNAME"), '"' + ctx.username + '"');
        script = std::regex_replace(script, std::regex("PASSWORD"), '"' + ctx.password + '"');
        script = std::regex_replace(script, std::regex("BUCKET"), '"' + ctx.bucket + '"');
        rb_eval_string_protect(script.c_str(), &status);
        if (status != 0) {
            VALUE rbError = rb_funcall(rb_gv_get("$!"), rb_intern("message"), 0);
            return std::string(StringValuePtr(rbError));
        }
        return {};
    }
};

#define TEST_PREAMBLE_RUBY                                                                                                                 \
    RUBY_INIT_STACK;                                                                                                                       \
    ruby_context ruby{};
