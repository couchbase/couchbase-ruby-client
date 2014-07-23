/* vim: ft=c et ts=8 sts=4 sw=4 cino=
 *
 *   Copyright 2012 Couchbase, Inc.
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

#ifndef _WIN32
#ifdef BUILD_EVENTMACHINE_PLUGIN

#include <libcouchbase/bsdio-inl.c>

VALUE cb_mEm;
VALUE cb_cEmSocket;
VALUE em_cPeriodicTimer;
VALUE cb_cEmEvent;
VALUE rb_mObSpace;
ID cb_id_add_timer;
ID cb_id_cancel_timer;
ID cb_id_detach;
ID cb_id_define_finalizer;
ID cb_id_iv_event;
ID cb_id_notify_readable_p;
ID cb_id_notify_writable_p;
ID cb_id_set_notify_readable;
ID cb_id_set_notify_writable;
ID cb_id_undefine_finalizer;
ID cb_id_watch;
VALUE cb_sym_clear_holder;
VALUE cb_sym_resume;

typedef struct rb_em_event rb_em_event;
typedef struct rb_em_loop rb_em_loop;
struct rb_em_event {
    lcb_socket_t socket;
    void *cb_data;
    void (*handler)(lcb_socket_t sock, short which, void *cb_data);
    VALUE holder;
    lcb_uint32_t usec;
    short canceled;
    short current_flags;
    short in_read_handler;
    short deferred_write_reset;
    VALUE self;
    rb_em_loop *loop;
};

struct rb_em_loop {
    VALUE fiber;
    struct cb_bucket_st *bucket;
};

    static void
rb_em_event_mark(void *p)
{
    if (p) {
        rb_em_event *ev = p;
        rb_gc_mark(ev->holder);
        rb_gc_mark(ev->loop->bucket->self);
    }
}

    static void
rb_em_event_free(void *p)
{
    if (p) {
        rb_em_event *ev = p;
        ev->self = 0;
        ev->holder = 0;
        ev->loop = NULL;
    }
}

    static void
rb_em_event_run_callback(rb_em_event *ev, short flags)
{
    if (ev->loop->fiber) {
        ev->current_flags = flags;
        rb_fiber_resume(ev->loop->fiber, 1, &ev->self);
    } else {
        ev->handler(ev->socket, flags, ev->cb_data);
    }
}

    static VALUE
rb_em_event_call(VALUE self)
{
    rb_em_event *ev;
    Data_Get_Struct(self, rb_em_event, ev);

    ev->holder = 0;
    rb_em_event_run_callback(ev, 0);

    if (!ev->canceled && !ev->holder) {
        ev->holder = rb_funcall_2(em_m, cb_id_add_timer, rb_float_new((double)ev->usec / 1.0e6), self);
    }

    return Qnil;
}

    static VALUE
rb_em_event_clear_holder(VALUE self)
{
    rb_em_event *ev;
    Data_Get_Struct(self, rb_em_event, ev);

    ev->holder = 0;

    return Qnil;
}

    static void
rb_em_event_setup_finalizer(rb_em_event *ev)
{
    rb_funcall_2(rb_mObSpace, cb_id_define_finalizer, ev->holder,
                 rb_obj_method(ev->self, cb_sym_clear_holder));
}

    static void
rb_em_event_clear_finalizer(rb_em_event *ev)
{
    rb_funcall_1(rb_mObSpace, cb_id_undefine_finalizer, ev->holder);
}

    static VALUE
rb_em_socket_notify_readable(VALUE self)
{
    VALUE event = rb_ivar_get(self, cb_id_iv_event);
    rb_em_event *ev;

    if (RTEST(event)) {
        Data_Get_Struct(event, rb_em_event, ev);
        ev->in_read_handler = 1;
        rb_em_event_run_callback(ev, LCB_READ_EVENT);
        ev->in_read_handler = 0;
    } else {
        rb_funcall_0(self, cb_id_detach);
    }

    return Qnil;
}

    static VALUE
rb_em_socket_notify_writable(VALUE self)
{
    VALUE event = rb_ivar_get(self, cb_id_iv_event);
    rb_em_event *ev;

    if (RTEST(event)) {
        Data_Get_Struct(event, rb_em_event, ev);
        rb_em_event_run_callback(ev, LCB_WRITE_EVENT);
        if (ev->deferred_write_reset) {
            ev->deferred_write_reset = 0;
            rb_funcall_1(ev->holder, cb_id_set_notify_writable, Qfalse);
        }
    } else {
        rb_funcall_0(self, cb_id_detach);
    }

    return Qnil;
}

    static void
cb_gc_em_loop_mark(void *p, struct cb_bucket_st *bucket)
{
    rb_em_loop *loop = p;
    rb_gc_mark(loop->fiber);
    (void)bucket;
}

    static rb_em_loop *
rb_em_loop_create(struct cb_bucket_st *bucket)
{
    rb_em_loop *loop = calloc(1, sizeof(*loop));
    loop->bucket = bucket;
    cb_gc_protect_ptr(bucket, loop, cb_gc_em_loop_mark);
    return loop;
}

    static void
rb_em_loop_destroy(rb_em_loop *loop)
{
    cb_gc_unprotect_ptr(loop->bucket, loop);
    free(loop);
}

    static void
initialize_event_machine_plugin() {
    VALUE em_cConnection;

    rb_mObSpace = rb_const_get(rb_cObject, rb_intern("ObjectSpace"));

    em_m = rb_const_get(rb_cObject, rb_intern("EM"));
    em_cConnection = rb_const_get(em_m, rb_intern("Connection"));
    em_cPeriodicTimer = rb_const_get(em_m, rb_intern("PeriodicTimer"));

    cb_mEm = rb_define_module_under(cb_mCouchbase, "EM");

    cb_cEmEvent = rb_define_class_under(cb_mEm, "Event", rb_cObject);
    rb_define_method(cb_cEmEvent, "call", rb_em_event_call, 0);
    rb_define_method(cb_cEmEvent, "clear_holder", rb_em_event_clear_holder, 0);

    cb_cEmSocket = rb_define_class_under(cb_mEm, "Socket", em_cConnection);
    rb_define_method(cb_cEmSocket, "notify_readable", rb_em_socket_notify_readable, 0);
    rb_define_method(cb_cEmSocket, "notify_writable", rb_em_socket_notify_writable, 0);

    cb_id_add_timer = rb_intern("add_timer");
    cb_id_cancel_timer = rb_intern("cancel_timer");
    cb_id_define_finalizer = rb_intern("define_finalizer");
    cb_id_detach = rb_intern("detach");
    cb_id_iv_event = rb_intern("@event");
    cb_id_notify_readable_p = rb_intern("notify_readable?");
    cb_id_notify_writable_p = rb_intern("notify_writable?");
    cb_id_set_notify_readable = rb_intern("notify_readable=");
    cb_id_set_notify_writable = rb_intern("notify_writable=");
    cb_id_undefine_finalizer = rb_intern("undefine_finalizer");
    cb_id_watch = rb_intern("watch");
    cb_sym_clear_holder = ID2SYM(rb_intern("clear_holder"));
    cb_sym_resume = ID2SYM(rb_intern("resume"));
}

    static void
cb_gc_em_event_mark(void *p, struct cb_bucket_st *bucket)
{
    rb_em_event *ev = p;
    rb_gc_mark(ev->self);
    (void)bucket;
}

    static void *
lcb_io_create_event(struct lcb_io_opt_st *iops)
{
    rb_em_loop *loop = iops->v.v0.cookie;
    rb_em_event *ev = calloc(1, sizeof(rb_em_event));
    VALUE res = Data_Wrap_Struct(cb_cEmEvent, rb_em_event_mark, rb_em_event_free, ev);
    cb_gc_protect_ptr(loop->bucket, ev, cb_gc_em_event_mark);
    ev->self = res;
    ev->loop = loop;
    ev->socket = -1;

    return ev;
}

    static inline void
rb_em_event_dealloc(rb_em_event *ev, rb_em_loop *loop)
{
    if (ev->self) {
        DATA_PTR(ev->self) = 0;
    }
    cb_gc_unprotect_ptr(loop->bucket, ev);
    free(ev);
}

    static int
lcb_io_update_event(struct lcb_io_opt_st *iops,
        lcb_socket_t sock,
        void *event,
        short flags,
        void *cb_data,
        void (*handler)(lcb_socket_t sock,
            short which,
            void *cb_data))
{
    rb_em_event *ev = event;

    if (ev->holder == 0) {
        ev->holder = rb_funcall_2(em_m, cb_id_watch, INT2FIX(sock), cb_cEmSocket);
        rb_ivar_set(ev->holder, cb_id_iv_event, ev->self);
        rb_em_event_setup_finalizer(ev);
    }

    ev->socket = sock;
    ev->cb_data = cb_data;
    ev->handler = handler;

    rb_funcall_1(ev->holder, cb_id_set_notify_readable, (flags & LCB_READ_EVENT) ? Qtrue : Qfalse);
    /* it is safe to reset WRITE event only from WRITE handler */
    if (ev->in_read_handler && (flags & LCB_WRITE_EVENT) == 0 && RTEST(rb_funcall_0(ev->holder, cb_id_notify_writable_p))) {
        ev->deferred_write_reset = 1;
    } else {
        rb_funcall_1(ev->holder, cb_id_set_notify_writable, (flags & LCB_WRITE_EVENT) ? Qtrue : Qfalse);
    }
    (void)iops;
    return 0;
}

    static void
lcb_io_delete_event(struct lcb_io_opt_st *iops,
        lcb_socket_t sock,
        void *event)
{
    rb_em_event *ev = event;
    if (ev->holder) {
        rb_funcall_1(ev->holder, cb_id_set_notify_readable, Qfalse);
        rb_funcall_1(ev->holder, cb_id_set_notify_writable, Qfalse);
    }
    (void)sock;
    (void)iops;
}

    static void
lcb_io_destroy_event(struct lcb_io_opt_st *iops,
        void *event)
{
    rb_em_loop *loop = iops->v.v0.cookie;
    rb_em_event *ev = event;
    if (ev->holder) {
        rb_em_event_clear_finalizer(ev);
        rb_ivar_set(ev->holder, cb_id_iv_event, Qfalse);
        rb_funcall_0(ev->holder, cb_id_detach);
        ev->holder = 0;
    }
    rb_em_event_dealloc(ev, loop);
}

#define lcb_io_create_timer lcb_io_create_event

    static int
lcb_io_update_timer(struct lcb_io_opt_st *iops, void *timer,
        lcb_uint32_t usec, void *cb_data,
        void (*handler)(lcb_socket_t sock, short which, void *cb_data))
{
    rb_em_event *ev = timer;

    if (ev->holder) {
        rb_funcall_1(em_m, cb_id_cancel_timer, ev->holder);
        ev->holder = 0;
    }

    ev->socket = (lcb_socket_t)-1;
    ev->cb_data = cb_data;
    ev->handler = handler;
    ev->usec = usec;
    ev->canceled = 0;
    ev->holder = rb_funcall_2(em_m, cb_id_add_timer, rb_float_new((double)usec / 1.0e6), ev->self);

    (void)iops;
    return 0;
}

    static void
lcb_io_delete_timer(struct lcb_io_opt_st *iops, void *timer)
{
    rb_em_event *ev = timer;

    if (ev->holder) {
        rb_funcall_1(em_m, cb_id_cancel_timer, ev->holder);
        ev->holder = 0;
    }
    ev->canceled = 1;
    (void)iops;
}

    static void
lcb_io_destroy_timer(struct lcb_io_opt_st *iops, void *timer)
{
    rb_em_loop *loop = iops->v.v0.cookie;
    rb_em_event *ev = timer;
    if (!ev->canceled) {
        lcb_io_delete_timer(iops, timer);
    }
    rb_em_event_dealloc(ev, loop);
}

    static void
lcb_io_run_event_loop(struct lcb_io_opt_st *iops)
{
    rb_em_loop *loop = iops->v.v0.cookie;
    VALUE fiber = rb_fiber_current();
    VALUE event;
    rb_em_event *ev;
    loop->fiber = fiber;
    for(;;) {
        event = rb_fiber_yield(0, NULL);
        if (!RTEST(event)) break;
        Data_Get_Struct(event, rb_em_event, ev);
        ev->handler(ev->socket, ev->current_flags, ev->cb_data);
    }
}

    static void
lcb_io_stop_event_loop(struct lcb_io_opt_st *iops)
{
    rb_em_loop *loop = iops->v.v0.cookie;
    VALUE fiber = loop->fiber;
    loop->fiber = 0;
    if (fiber) {
        VALUE method = rb_obj_method(fiber, cb_sym_resume);
        rb_funcall_1(em_m, cb_id_next_tick, method);
    }
}

    static void
lcb_destroy_io_opts(struct lcb_io_opt_st *iops)
{
    rb_em_loop_destroy((rb_em_loop*)iops->v.v0.cookie);
}

    LIBCOUCHBASE_API lcb_error_t
cb_create_ruby_em_io_opts(int version, lcb_io_opt_t *io, void *arg)
{
    struct lcb_io_opt_st *ret;
    rb_em_loop *loop;
    struct cb_bucket_st *bucket = arg;

    if (version != 0) {
        return LCB_PLUGIN_VERSION_MISMATCH;
    }

    if (!em_m) initialize_event_machine_plugin();

    ret = calloc(1, sizeof(*ret));
    if (ret == NULL) {
        free(ret);
        return LCB_CLIENT_ENOMEM;
    }

    ret->version = 0;
    ret->dlhandle = NULL;
    ret->destructor = lcb_destroy_io_opts;
    /* consider that struct isn't allocated by the library,
     * `need_cleanup' flag might be set in lcb_create() */
    ret->v.v0.need_cleanup = 0;
    wire_lcb_bsd_impl(ret);
    ret->v.v0.delete_event = lcb_io_delete_event;
    ret->v.v0.destroy_event = lcb_io_destroy_event;
    ret->v.v0.create_event = lcb_io_create_event;
    ret->v.v0.update_event = lcb_io_update_event;

    ret->v.v0.delete_timer = lcb_io_delete_timer;
    ret->v.v0.destroy_timer = lcb_io_destroy_timer;
    ret->v.v0.create_timer = lcb_io_create_timer;
    ret->v.v0.update_timer = lcb_io_update_timer;

    ret->v.v0.run_event_loop = lcb_io_run_event_loop;
    ret->v.v0.stop_event_loop = lcb_io_stop_event_loop;

    loop = rb_em_loop_create(bucket);
    ret->v.v0.cookie = loop;

    *io = ret;
    return LCB_SUCCESS;
}

#endif
#endif
