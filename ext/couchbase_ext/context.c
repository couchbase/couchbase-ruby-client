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

static void
cb_context_mark(void *p, struct cb_bucket_st *bucket)
{
    struct cb_context_st *ctx = p;
    rb_gc_mark(ctx->proc);
    rb_gc_mark(ctx->rv);
    rb_gc_mark(ctx->exception);
    rb_gc_mark(ctx->observe_options);
    rb_gc_mark(ctx->transcoder);
    rb_gc_mark(ctx->transcoder_opts);
    rb_gc_mark(ctx->operation);
    rb_gc_mark(ctx->headers_val);
    (void)bucket;
}

struct cb_context_st *
cb_context_alloc(struct cb_bucket_st *bucket)
{
    struct cb_context_st *ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        rb_raise(cb_eClientNoMemoryError, "failed to allocate memory for context");
    }
    cb_gc_protect_ptr(bucket, ctx, cb_context_mark);
    ctx->bucket = bucket;
    ctx->exception = Qnil;
    return ctx;
}

struct cb_context_st *
cb_context_alloc_common(struct cb_bucket_st *bucket, size_t nqueries)
{
    struct cb_context_st *ctx = cb_context_alloc(bucket);
    ctx->nqueries = nqueries;
    ctx->rv = rb_hash_new();
    ctx->proc = Qnil;
    return ctx;
}

void
cb_context_free(struct cb_context_st *ctx)
{
    cb_gc_unprotect_ptr(ctx->bucket, ctx);
    free(ctx);
}
