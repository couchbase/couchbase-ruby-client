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

    void
cb_timer_free(void *ptr)
{
    xfree(ptr);
}

    void
cb_timer_mark(void *ptr)
{
    struct cb_timer_st *timer = ptr;
    if (timer) {
        rb_gc_mark(timer->callback);
    }
}

    VALUE
cb_timer_alloc(VALUE klass)
{
    VALUE obj;
    struct cb_timer_st *timer;

    /* allocate new bucket struct and set it to zero */
    obj = Data_Make_Struct(klass, struct cb_timer_st, cb_timer_mark,
            cb_timer_free, timer);
    return obj;
}

/*
 * Returns a string containing a human-readable representation of the
 * Timer.
 *
 * @since 1.2.0.dp6
 *
 * @return [String]
 */
    VALUE
cb_timer_inspect(VALUE self)
{
    VALUE str;
    struct cb_timer_st *tm = DATA_PTR(self);
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 20, ":%p", (void *)self);
    rb_str_buf_cat2(str, buf);
    snprintf(buf, 100, " timeout:%u periodic:%s>",
            tm->usec, tm->periodic ? "true" : "false");
    rb_str_buf_cat2(str, buf);

    return str;
}

/*
 * Cancel the timer.
 *
 * @since 1.2.0.dp6
 *
 * This operation makes sense for periodic timers or if one need to cancel
 * regular timer before it will be triggered.
 *
 * @example Cancel periodic timer
 *   n = 1
 *   c.run do
 *     tm = c.create_periodic_timer(500000) do
 *       c.incr("foo") do
 *         if n == 5
 *           tm.cancel
 *         else
 *           n += 1
 *         end
 *       end
 *     end
 *   end
 *
 * @return [String]
 */
    VALUE
cb_timer_cancel(VALUE self)
{
    struct cb_timer_st *tm = DATA_PTR(self);
    lcb_timer_destroy(tm->bucket->handle, tm->timer);
    return self;
}

    static VALUE
trigger_timer(VALUE timer)
{
    struct cb_timer_st *tm = DATA_PTR(timer);
    return cb_proc_call(tm->bucket, tm->callback, 1, timer);
}

    static void
timer_callback(lcb_timer_t timer, lcb_t instance,
        const void *cookie)
{
    struct cb_timer_st *tm = (struct cb_timer_st *)cookie;
    int error = 0;

    rb_protect(trigger_timer, tm->self, &error);
    if (error) {
        lcb_timer_destroy(instance, timer);
    }
    (void)cookie;
}

/*
 * Initialize new Timer
 *
 * @since 1.2.0
 *
 * The timers could used to trigger reccuring events or implement timeouts.
 * The library will call given block after time interval pass.
 *
 * @param bucket [Bucket] the connection object
 * @param interval [Fixnum] the interval in microseconds
 * @param options [Hash]
 * @option options [Boolean] :periodic (false) set it to +true+ if the timer
 *   should be triggered until it will be canceled.
 *
 * @yieldparam [Timer] timer the current timer
 *
 * @example Create regular timer for 0.5 second
 *   c.run do
 *     Couchbase::Timer.new(c, 500000) do
 *       puts "ding-dong"
 *     end
 *   end
 *
 * @example Create periodic timer
 *   n = 10
 *   c.run do
 *     Couchbase::Timer.new(c, 500000, :periodic => true) do |tm|
 *       puts "#{n}"
 *       n -= 1
 *       tm.cancel if n.zero?
 *     end
 *   end
 *
 *
 * @return [Couchbase::Timer]
 */
    VALUE
cb_timer_init(int argc, VALUE *argv, VALUE self)
{
    struct cb_timer_st *tm = DATA_PTR(self);
    VALUE bucket, opts, timeout, exc, cb;
    lcb_error_t err;

    rb_need_block();
    rb_scan_args(argc, argv, "21&", &bucket, &timeout, &opts, &cb);

    if (!RTEST(rb_obj_is_kind_of(bucket, cb_cBucket))) {
        rb_raise(rb_eTypeError, "wrong argument type (expected Couchbase::Bucket)");
    }
    tm->self = self;
    tm->callback = cb;
    tm->usec = NUM2ULONG(timeout);
    tm->bucket = DATA_PTR(bucket);
    if (opts != Qnil) {
        Check_Type(opts, T_HASH);
        tm->periodic = RTEST(rb_hash_aref(opts, cb_sym_periodic));
    }
    tm->timer = lcb_timer_create(tm->bucket->handle, tm, tm->usec,
            tm->periodic, timer_callback, &err);
    exc = cb_check_error(err, "failed to attach the timer", Qnil);
    if (exc != Qnil) {
        rb_exc_raise(exc);
    }

    return self;
}

