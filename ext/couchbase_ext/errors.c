/* vim: ft=c et ts=8 sts=4 sw=4 cino=
 *
 *   Copyright 2018 Couchbase, Inc.
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

VALUE
cb_exc_new_at(VALUE klass, lcb_error_t code, const char *file, int line, const char *fmt, ...)
{
    VALUE exc, str;
    const char *errstr = NULL;
    va_list args;

    va_start(args, fmt);
    str = rb_vsprintf(fmt, args);
    va_end(args);

    if (code > 0) {
        errstr = lcb_strerror_short(code);
        rb_str_catf(str, ": %s", errstr);
    }

    rb_str_buf_cat_ascii(str, " [");
    while (*file == '.' || *file == '/') {
        file++;
    }
    rb_str_buf_cat_ascii(str, file);
    rb_str_catf(str, ":%d]", line);

    exc = rb_exc_new_str(klass, str);
    if (code > 0) {
        rb_ivar_set(exc, rb_intern("@code"), INT2FIX(code));
        rb_ivar_set(exc, rb_intern("@message"), rb_str_new_cstr(lcb_strerror(NULL, code)));
        if (errstr && errstr[0] != '<') {
            const char *endp = strchr(errstr, ' ');
            if (endp) {
                rb_ivar_set(exc, rb_intern("@name"), rb_str_new(errstr, endp - errstr));
            }
        }
    }
    return exc;
}

void
cb_raise_at(VALUE klass, lcb_error_t code, const char *file, int line, const char *fmt, ...)
{
    VALUE exc, str;
    const char *errstr = NULL;
    va_list args;

    va_start(args, fmt);
    str = rb_vsprintf(fmt, args);
    va_end(args);

    if (code > 0) {
        errstr = lcb_strerror_short(code);
        rb_str_catf(str, ": %s", errstr);
    }

    rb_str_buf_cat_ascii(str, " [");
    while (*file == '.' || *file == '/') {
        file++;
    }
    rb_str_buf_cat_ascii(str, file);
    rb_str_catf(str, ":%d]", line);

    exc = rb_exc_new_str(klass, str);
    if (code > 0) {
        rb_ivar_set(exc, rb_intern("@code"), INT2FIX(code));
        rb_ivar_set(exc, rb_intern("@message"), rb_str_new_cstr(lcb_strerror(NULL, code)));
        if (errstr && errstr[0] != '<') {
            const char *endp = strchr(errstr, ' ');
            if (endp) {
                rb_ivar_set(exc, rb_intern("@name"), rb_str_new(errstr, endp - errstr));
            }
        }
    }
    rb_exc_raise(exc);
}

#define ERROR_CLASS_CHECKER(macro, method)                                                                             \
    static VALUE method(VALUE self)                                                                                    \
    {                                                                                                                  \
        VALUE code = rb_ivar_get(self, rb_intern("@code"));                                                            \
        if (TYPE(code) != T_FIXNUM) {                                                                                  \
            return Qnil;                                                                                               \
        }                                                                                                              \
        return macro(FIX2INT(code)) ? Qtrue : Qfalse;                                                                  \
    }

ERROR_CLASS_CHECKER(LCB_EIFINPUT, cb_library_error_input_p);
ERROR_CLASS_CHECKER(LCB_EIFNET, cb_library_error_network_p);
ERROR_CLASS_CHECKER(LCB_EIFFATAL, cb_library_error_fatal_p);
ERROR_CLASS_CHECKER(LCB_EIFTMP, cb_library_error_transient_p);
ERROR_CLASS_CHECKER(LCB_EIFDATA, cb_library_error_data_p);
ERROR_CLASS_CHECKER(LCB_EIFPLUGIN, cb_library_error_plugin_p);
ERROR_CLASS_CHECKER(LCB_EIFSUBDOC, cb_library_error_subdoc_p);
ERROR_CLASS_CHECKER(LCB_EIFSRVLOAD, cb_library_error_server_load_p);
ERROR_CLASS_CHECKER(LCB_EIFSRVGEN, cb_library_error_server_generated_p);

VALUE cb_eLibraryError;

void
init_library_error()
{
    cb_eLibraryError = rb_const_get(cb_mCouchbase, rb_intern("LibraryError"));
    rb_define_method(cb_eLibraryError, "input?", cb_library_error_input_p, 0);
    rb_define_method(cb_eLibraryError, "network?", cb_library_error_network_p, 0);
    rb_define_method(cb_eLibraryError, "fatal?", cb_library_error_fatal_p, 0);
    rb_define_method(cb_eLibraryError, "transient?", cb_library_error_transient_p, 0);
    rb_define_method(cb_eLibraryError, "data?", cb_library_error_data_p, 0);
    rb_define_method(cb_eLibraryError, "plugin?", cb_library_error_plugin_p, 0);
    rb_define_method(cb_eLibraryError, "subdoc?", cb_library_error_subdoc_p, 0);
    rb_define_method(cb_eLibraryError, "server_load?", cb_library_error_server_load_p, 0);
    rb_define_method(cb_eLibraryError, "server_generated?", cb_library_error_server_generated_p, 0);

#define X(c, v, t, s) rb_const_set(cb_eLibraryError, rb_intern(#c), INT2FIX(v));
    LCB_XERR(X)
#undef X
}
