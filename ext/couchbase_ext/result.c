/* vim: ft=c et ts=8 sts=4 sw=4 cino=
 *
 *   Copyright 2011, 2012 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "couchbase_ext.h"

/*
 * Check if result of operation was successful.
 *
 * @since 1.0.0
 *
 * @return [true, false] +false+ if there is an +error+ object attached,
 *   +false+ otherwise.
 */
    VALUE
cb_result_success_p(VALUE self)
{
    return RTEST(rb_ivar_get(self, id_iv_error)) ? Qfalse : Qtrue;
}

/*
 * Returns a string containing a human-readable representation of the Result.
 *
 * @since 1.0.0
 *
 * @return [String]
 */
    VALUE
cb_result_inspect(VALUE self)
{
    VALUE str, attr, error;
    char buf[100];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 100, ":%p", (void *)self);
    rb_str_buf_cat2(str, buf);

    attr = rb_ivar_get(self, id_iv_error);
    if (RTEST(attr)) {
        error = rb_ivar_get(attr, id_iv_error);
    } else {
        error = INT2FIX(0);
    }
    rb_str_buf_cat2(str, " error=0x");
    rb_str_append(str, rb_funcall(error, id_to_s, 1, INT2FIX(16)));

    attr = rb_ivar_get(self, id_iv_operation);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " operation=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_key);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " key=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_status);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " status=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_cas);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " cas=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_flags);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " flags=0x");
        rb_str_append(str, rb_funcall(attr, id_to_s, 1, INT2FIX(16)));
    }

    attr = rb_ivar_get(self, id_iv_node);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " node=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_from_master);
    if (attr != Qnil) {
        rb_str_buf_cat2(str, " from_master=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_time_to_persist);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " time_to_persist=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_time_to_replicate);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " time_to_replicate=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_headers);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " headers=");
        rb_str_append(str, rb_inspect(attr));
    }

    rb_str_buf_cat2(str, ">");

    return str;
}


