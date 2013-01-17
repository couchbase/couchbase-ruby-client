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

#ifndef HAVE_RB_THREAD_BLOCKING_REGION
#include <rubysig.h>
#endif
#include <errno.h>
#ifdef HAVE_POLL
#include <poll.h>
#endif

/* events sorted array */
typedef struct rb_mt_event rb_mt_event;
struct rb_mt_event {
    void *cb_data;
    void (*handler)(lcb_socket_t sock, short which, void *cb_data);
    lcb_socket_t socket;
    int loop_index;
    short flags;
    short actual_flags;
    short inserted;
    rb_mt_event *next;
};

typedef struct rb_mt_socket_list rb_mt_socket_list;
struct rb_mt_socket_list {
    lcb_socket_t socket;
    short flags;
    rb_mt_event *first;
};

typedef struct rb_mt_events rb_mt_events;
struct rb_mt_events {
    uint32_t capa;
    uint32_t count;
    rb_mt_socket_list *sockets;
};

    static int
events_init(rb_mt_events *events)
{
    rb_mt_socket_list *new_socks = malloc(4 * sizeof(*new_socks));
    if (new_socks == NULL) {
        return 0;
    }
    events->capa = 4;
    events->count = 0;
    events->sockets = new_socks;
    return 1;
}

    static void
events_finalize(rb_mt_events *events)
{
    if (events->sockets) {
        uint32_t i;
        for(i = 0; i < events->count; i++) {
            rb_mt_socket_list *list = &events->sockets[i];
            while(list->first) {
                rb_mt_event *next = list->first->next;
                free(list->first);
                list->first = next;
            }
        }
        free(events->sockets);
        events->sockets = NULL;
    }
    events->capa = 0;
    events->count = 0;
}

    static uint32_t
events_index(rb_mt_events *events, lcb_socket_t socket)
{
    uint32_t m, l = 0, r = events->count;
    while(l < r) {
        m = l + (r - l) / 2;
        if (events->sockets[m].socket >= socket) {
            r = m;
        } else {
            l = m + 1;
        }
    }
    return l;
}

    static void
events_insert(rb_mt_events *events, rb_mt_event *event)
{
    uint32_t i = events_index(events, event->socket);
    rb_mt_socket_list *list = &events->sockets[i];
    if (i == events->count || list->socket != event->socket) {
        if (events->capa == events->count) {
            uint32_t new_capa = events->capa << 1;
            rb_mt_socket_list *new_socks = realloc(events->sockets, new_capa * sizeof(*new_socks));
            if (new_socks == NULL) {
                rb_raise(cb_eClientNoMemoryError, "failed to allocate memory for events array");
            }
            events->sockets = new_socks;
            events->capa = new_capa;
            list = &events->sockets[i];
        }
        if (i < events->count) {
            MEMMOVE(events->sockets+i+1, events->sockets+i, rb_mt_socket_list, events->count - i);
        }
        events->count++;
        list->socket = event->socket;
        list->flags = event->flags;
        list->first = event;
        event->next = NULL;
    } else {
        list->flags |= event->flags;
        event->next = list->first;
        list->first = event;
    }
    event->inserted = 1;
}

    static void
event_list_fix_flags(rb_mt_socket_list *list)
{
    short flags = 0;
    rb_mt_event *event = list->first;
    while (event) {
        flags |= event->flags;
        event = event->next;
    }
    list->flags = flags;
}

    static void
events_remove(rb_mt_events *events, rb_mt_event *event)
{
    uint32_t i = events_index(events, event->socket);
    rb_mt_socket_list *list = &events->sockets[i];
    rb_mt_event **next;
    if (list->socket != event->socket) {
        rb_raise(rb_eIndexError, "There is no socket in event loop");
    }
    next = &list->first;
    for(;;) {
        if (*next == NULL) {
            rb_raise(rb_eIndexError, "There is no event in event loop");
        }
        if (*next == event) {
            *next = event->next;
            event->next = NULL;
            event->inserted = 0;
            break;
        }
        next = &event->next;
    }
    if (list->first == NULL) {
        MEMMOVE(events->sockets + i, events->sockets + i + 1, rb_mt_socket_list, events->count - i - 1);
        events->count--;
    } else {
        event_list_fix_flags(list);
    }
}

    static void
events_fix_flags(rb_mt_events *events, lcb_socket_t socket)
{
    uint32_t i = events_index(events, socket);
    rb_mt_socket_list *list = &events->sockets[i];
    if (list->socket != socket) {
        rb_raise(rb_eIndexError, "There is no socket in event loop");
    }
    event_list_fix_flags(list);
}

    static inline lcb_socket_t
events_max_fd(rb_mt_events *events)
{
    if (events->count) {
        return events->sockets[events->count - 1].socket;
    } else {
        return -1;
    }
}

/* events sorted array end */

/* timers heap */
typedef struct rb_mt_timer rb_mt_timer;
struct rb_mt_timer {
    void *cb_data;
    void (*handler)(lcb_socket_t sock, short which, void *cb_data);
    int index;
    hrtime_t ts;
    hrtime_t period;
};

typedef struct rb_mt_timers rb_mt_timers;
struct rb_mt_timers {
    uint32_t capa;
    uint32_t count;
    rb_mt_timer **timers;
};

    static int
timers_init(rb_mt_timers *timers)
{
    rb_mt_timer **new_timers = malloc(4 * sizeof(*new_timers));
    if (new_timers == NULL) {
        return 0;
    }
    timers->capa = 4;
    timers->count = 0;
    timers->timers = new_timers;
    return 1;
}

    static void
timers_finalize(rb_mt_timers *timers)
{
    if (timers->timers) {
        uint32_t i;
        for(i = 0; i < timers->count; i++) {
            free(timers->timers[i]);
        }
        free(timers->timers);
        timers->timers = NULL;
    }
    timers->count = 0;
    timers->capa = 0;
}

#define tms_at(_timers, at) (_timers)->timers[(at)]
#define tms_ts_at(timers, at) tms_at((timers), (at))->ts

    static void
timers_move_last(rb_mt_timers *timers, uint32_t to)
{
    if (to < timers->count - 1) {
        rb_mt_timer *last = tms_at(timers, timers->count - 1);
        tms_at(timers, to) = last;
        last->index = to;
    }
    timers->count--;
}

    static inline void
timers_swap(rb_mt_timers *timers, uint32_t i, uint32_t j)
{
    rb_mt_timer *itmp = tms_at(timers, j);
    rb_mt_timer *jtmp = tms_at(timers, i);
    tms_at(timers, i) = itmp;
    tms_at(timers, j) = jtmp;
    itmp->index = i;
    jtmp->index = j;
}

static void timers_heapify_up(rb_mt_timers *timers, uint32_t pos);

    static void
timers_insert(rb_mt_timers *timers, rb_mt_timer *timer)
{
    if (timers->count == timers->capa) {
        rb_mt_timer **new_timers;
        size_t new_capa = timers->capa << 1;
        new_timers = realloc(timers->timers, new_capa * sizeof(rb_mt_timer*));
        if (new_timers == NULL) {
            rb_raise(cb_eClientNoMemoryError, "failed to allocate memory for timers heap");
        }
        timers->timers = new_timers;
        timers->capa = new_capa;
    }
    tms_at(timers, timers->count) = timer;
    timer->index = timers->count;
    timers->count++;
    timers_heapify_up(timers, timer->index);
}

    static void
timers_heapify_up(rb_mt_timers *timers, uint32_t pos)
{
    hrtime_t cur_ts = tms_ts_at(timers, pos);
    uint32_t higher = (pos - 1) / 2;
    while (pos && tms_ts_at(timers, higher) > cur_ts) {
        timers_swap(timers, higher, pos);
        pos = higher;
        higher = (pos - 1) / 2;
    }
}

    static void
timers_heapify_down(rb_mt_timers *timers, uint32_t pos)
{
    uint32_t count = timers->count;
    uint32_t middle = (timers->count - 2) / 2;
    hrtime_t cur_ts = tms_ts_at(timers, pos);
    if (count == 1) return;
    while (pos <= middle) {
        uint32_t min_pos = pos;
        hrtime_t ch_ts, min_ts = cur_ts;

        if ((ch_ts = tms_ts_at(timers, pos * 2 + 1)) < min_ts) {
            min_pos = pos * 2 + 1;
            min_ts = ch_ts;
        }

        if (pos * 2 + 2 < count && tms_ts_at(timers, pos * 2 + 2) < min_ts) {
            min_pos = pos * 2 + 2;
        }

        if (min_pos == pos) break;
        timers_swap(timers, pos, min_pos);
        pos = min_pos;
    }
}

    static void
timers_heapify_item(rb_mt_timers *timers, uint32_t pos)
{
    if (pos && tms_ts_at(timers, pos) < tms_ts_at(timers, (pos - 1) / 2)) {
        timers_heapify_up(timers, pos);
    } else {
        timers_heapify_down(timers, pos);
    }
}

    static inline hrtime_t
timers_minimum(rb_mt_timers *timers)
{
    if (timers->count) {
        return tms_ts_at(timers, 0);
    } else {
        return 0;
    }
}

    static inline rb_mt_timer *
timers_first(rb_mt_timers *timers)
{
    if (timers->count) {
        return tms_at(timers, 0);
    } else {
        return 0;
    }
}

    static void
timers_remove_timer(rb_mt_timers *timers, rb_mt_timer *timer)
{
    uint32_t at = timer->index;
    timer->index = -1;
    if (at < timers->count - 1) {
        timers_move_last(timers, at);
        timers_heapify_item(timers, at);
    } else {
        timers->count--;
    }
}

    static void
timers_run(rb_mt_timers *timers, hrtime_t now)
{
    hrtime_t next_time = timers_minimum(timers);
    while (next_time && next_time < now) {
        rb_mt_timer *first = timers_first(timers);

        first->ts = now + first->period;
        timers_heapify_item(timers, 0);

        first->handler(-1, 0, first->cb_data);

        next_time = timers_minimum(timers);
    }
}
/* timers heap end */

/* callbacks array */
typedef struct rb_mt_callbacks rb_mt_callbacks;
struct rb_mt_callbacks {
    uint32_t capa;
    uint32_t count;
    rb_mt_event **events;
};

    static int
callbacks_init(rb_mt_callbacks *callbacks)
{
    rb_mt_event **new_events = calloc(4, sizeof(*new_events));
    if (new_events == NULL) {
        return 0;
    }
    callbacks->events = new_events;
    callbacks->capa = 4;
    callbacks->count = 0;
    return 1;
}

    static void
callbacks_finalize(rb_mt_callbacks *callbacks)
{
    if (callbacks->events) {
        free(callbacks->events);
        callbacks->events = NULL;
    }
    callbacks->capa = 0;
    callbacks->count = 0;
}

    static void
callbacks_push(rb_mt_callbacks *callbacks, rb_mt_event *event)
{
    if (callbacks->count == callbacks->capa) {
        uint32_t new_capa = callbacks->capa << 1;
        rb_mt_event **new_events = realloc(callbacks->events, new_capa * sizeof(*new_events));
        if (new_events == NULL) {
            rb_raise(cb_eClientNoMemoryError, "failed to allocate memory for callbacks array");
        }
        callbacks->capa = new_capa;
        callbacks->events = new_events;
    }
    callbacks->events[callbacks->count] = event;
    callbacks->count++;
}

    static void
callbacks_remove(rb_mt_callbacks *callbacks, rb_mt_event *event)
{
    int i = event->loop_index;
    if (i >= 0) {
        if (callbacks->events[i] != event) {
            rb_raise(rb_eIndexError, "callback index belongs to different callback");
        }
        event->loop_index = -1;
        callbacks->events[i] = NULL;
    }
}

    static void
callbacks_run(rb_mt_callbacks *callbacks)
{
    uint32_t i;
    for(i = 0; i < callbacks->count; i++) {
        rb_mt_event *cb = callbacks->events[i];
        if (cb) {
            cb->handler(cb->socket, cb->actual_flags, cb->cb_data);
        }
    }
    callbacks->count = 0;
}

    static void
callbacks_clean(rb_mt_callbacks *callbacks)
{
    uint32_t i;
    for(i = 0; i < callbacks->count; i++) {
        if (callbacks->events[i]) {
            callbacks->events[i]->loop_index = -1;
            callbacks->events[i] = NULL;
        }
    }
    callbacks->count = 0;
}
/* callbacks array end */

typedef struct rb_mt_loop rb_mt_loop;
struct rb_mt_loop {
    rb_mt_events events;
    rb_mt_timers timers;
    rb_mt_callbacks callbacks;
    short run;
};

    static rb_mt_loop*
loop_create()
{
    rb_mt_loop *loop = calloc(1, sizeof(*loop));
    if (loop == NULL) return NULL;
    if (!events_init(&loop->events)) goto free_loop;
    if (!timers_init(&loop->timers)) goto free_events;
    if (!callbacks_init(&loop->callbacks)) goto free_timers;
    return loop;

free_timers:
    timers_finalize(&loop->timers);
free_events:
    events_finalize(&loop->events);
free_loop:
    free(loop);
    return NULL;
}

    static void
loop_destroy(rb_mt_loop *loop)
{
    events_finalize(&loop->events);
    timers_finalize(&loop->timers);
    callbacks_finalize(&loop->callbacks);
    free(loop);
}

    static void
loop_remove_event(rb_mt_loop *loop, rb_mt_event *event)
{
    if (event->inserted) {
        events_remove(&loop->events, event);
    }
    callbacks_remove(&loop->callbacks, event);
}

    static void
loop_enque_events(rb_mt_callbacks *callbacks, rb_mt_event *sock, short flags)
{
    while (sock) {
        short actual = sock->flags & flags;
        if (actual) {
            sock->actual_flags = actual;
            callbacks_push(callbacks, (rb_mt_event*)sock);
        }
        sock = sock->next;
    }
}

/* loop select implementation */
#ifndef HAVE_RB_THREAD_FD_SELECT
typedef fd_set rb_fdset_t;
#define rb_fd_init   FD_ZERO
#define rb_fd_set    FD_SET
#define rb_fd_isset  FD_ISSET
#define rb_fd_term(set)  (void)0
#define rb_thread_fd_select rb_thread_select
#endif

typedef struct loop_select_arg {
    rb_mt_loop *loop;
    rb_fdset_t in, out;
} ls_arg;

    static void
ls_arg_free(void *p) {
    ls_arg *args = p;
    if (args) {
        rb_fd_term(&args->in);
        rb_fd_term(&args->out);
        xfree(args);
    }
}

    static VALUE
ls_arg_alloc(ls_arg **args)
{
    return Data_Make_Struct(rb_cObject, ls_arg, 0, ls_arg_free, *args);
}

    static VALUE
loop_run_select(VALUE argp)
{
    ls_arg *args = (ls_arg*) argp;
    rb_mt_loop *loop = args->loop;
    rb_fdset_t *in = NULL, *out = NULL;
    struct timeval timeout;
    struct timeval *timeoutp = NULL;
    int result, max = 0;
    hrtime_t now, next_time;

    next_time = timers_minimum(&loop->timers);
    if (next_time) {
        now = gethrtime();
        if (next_time <= now) {
            timeout.tv_sec = 0;
            timeout.tv_usec = 0;
        } else {
            hrtime_t hrto = (next_time - now) / 1000;
            timeout.tv_sec = (long)(hrto / 1000000);
            timeout.tv_usec = (long)(hrto % 1000000);
        }
        timeoutp = &timeout;
    }

    if (loop->events.count) {
        uint32_t i;
        rb_fd_init(&args->in);
        rb_fd_init(&args->out);
        for(i = 0; i < loop->events.count; i++) {
            rb_mt_socket_list *list = &loop->events.sockets[i];
            if (list->flags & LCB_READ_EVENT) {
                in = &args->in;
                rb_fd_set(list->socket, in);
            }
            if (list->flags & LCB_WRITE_EVENT) {
                out = &args->out;
                rb_fd_set(list->socket, out);
            }
        }
        max = events_max_fd(&loop->events) + 1;
    }

    result = rb_thread_fd_select(max, in, out, NULL, timeoutp);

    if (result < 0) {
        rb_sys_fail("rb_thread_fd_select");
    }
    /* fix current time so that socket callbacks will not cause timers timeouts */
    if (next_time) {
        now = gethrtime();
    }

    if (result > 0) {
        uint32_t i;
        for(i = 0; i < loop->events.count && result; i++) {
            rb_mt_socket_list *list = loop->events.sockets + i;
            rb_mt_event *sock = list->first;
            short flags = 0;
            if (in && rb_fd_isset(list->socket, in)) {
                flags |= LCB_READ_EVENT;
                result--;
            }
            if (out && rb_fd_isset(list->socket, out)) {
                flags |= LCB_WRITE_EVENT;
                result--;
            }
            if (flags) {
                loop_enque_events(&loop->callbacks, sock, flags);
            }
        }
        callbacks_run(&loop->callbacks);
    }

    if (next_time) {
        timers_run(&loop->timers, now);
    }
    if (loop->events.count == 0 && loop->timers.count == 0) {
        loop->run = 0;
    }
    return Qnil;
}

    static VALUE
loop_select_cleanup(VALUE argp)
{
    ls_arg *args = DATA_PTR(argp);
    if (args) {
        callbacks_clean(&args->loop->callbacks);
        ls_arg_free(args);
        DATA_PTR(argp) = 0;
    }
    return Qnil;
}
/* loop select implementaion end */

/* loop poll implementation */
#ifdef HAVE_POLL
/* code influenced by ruby's source and cool.io */
#define POLLIN_SET (POLLIN | POLLHUP | POLLERR)
#define POLLOUT_SET (POLLOUT | POLLHUP | POLLERR)
#define HRTIME_INFINITY ((hrtime_t)~0)

#ifdef HAVE_PPOLL
    static int
xpoll(struct pollfd *fds, nfds_t nfds, hrtime_t timeout)
{
    if (timeout != HRTIME_INFINITY) {
        struct timespec ts;
        ts.tv_sec = (long)(timeout / (1000 * 1000 * 1000));
        ts.tv_nsec = (long)(timeout % (1000 * 1000 * 1000));
        return ppoll(fds, nfds, &ts, NULL);
    }
    return ppoll(fds, nfds, NULL, NULL);
}
#else
#define TIMEOUT_MAX ((hrtime_t)(((unsigned int)~0) >> 1))
    static int
xpoll(struct pollfd *fds, nfds_t nfds, hrtime_t timeout)
{
    int ts = -1;
    if (timeout != HRTIME_INFINITY) {
        timeout = (timeout + 999999) / (1000 * 1000);
        if (timeout <= TIMEOUT_MAX) {
            ts = (int)timeout;
        }
    }
    return poll(fds, nfds, ts);
}
#endif

typedef struct poll_args lp_arg;
struct poll_args {
    rb_mt_loop *loop;
    struct pollfd *fds;
    nfds_t nfd;
    hrtime_t ts;
    int result;
    int lerrno;
};

    static void
lp_arg_free(void *p)
{
    lp_arg *args = p;
    if (args) {
        if (args->fds) {
            free(args->fds);
        }
        xfree(args);
    }
}

    static VALUE
lp_arg_alloc(lp_arg **args)
{
    return Data_Make_Struct(rb_cObject, lp_arg, 0, lp_arg_free, *args);
}

#ifdef HAVE_RB_THREAD_BLOCKING_REGION
    static VALUE
loop_blocking_poll(void *argp)
{
    lp_arg *args = argp;
    args->result = xpoll(args->fds, args->nfd, args->ts);
    if (args->result < 0) args->lerrno = errno;
    return Qnil;
}
#endif

    static VALUE
loop_run_poll(VALUE argp)
{
    lp_arg *args = (lp_arg*)argp;
    rb_mt_loop *loop = args->loop;
    hrtime_t now, next_time;

    if (loop->events.count) {
        uint32_t i;
        args->fds = calloc(loop->events.count, sizeof(struct pollfd));
        if (args->fds == NULL) {
            rb_raise(cb_eClientNoMemoryError, "failed to allocate memory for pollfd");
        }
        for(i = 0; i < loop->events.count; i++) {
            rb_mt_socket_list *list = &loop->events.sockets[i];
            args->fds[i].fd = list->socket;
            args->fds[i].events =
                (list->flags & LCB_READ_EVENT ? POLLIN : 0) |
                (list->flags & LCB_WRITE_EVENT ? POLLOUT : 0);
        }
        args->nfd = loop->events.count;
    }

retry:
    next_time = timers_minimum(&loop->timers);
    if (next_time) {
        now = gethrtime();
        args->ts = next_time <= now ? 0 : next_time - now;
    } else {
        args->ts = HRTIME_INFINITY;
    }

#ifdef HAVE_RB_THREAD_BLOCKING_REGION
    rb_thread_blocking_region(loop_blocking_poll, args, RUBY_UBF_PROCESS, NULL);
#else
    if (rb_thread_alone()) {
        TRAP_BEG;
        args->result = xpoll(args->fds, args->nfd, args->ts);
        if (args->result < 0) args->lerrno = errno;
        TRAP_END;
    } else {
        /* 5 millisecond pause */
        hrtime_t mini_pause = 5000000;
        int exact = 0;
        if (args->ts != HRTIME_INFINITY && args->ts < mini_pause) {
            mini_pause = args->ts;
            exact = 1;
        }
        TRAP_BEG;
        args->result = xpoll(args->fds, args->nfd, mini_pause);
        if (args->result < 0) args->lerrno = errno;
        TRAP_END;
        if (args->result == 0 && !exact) {
            args->result = -1;
            args->lerrno = EINTR;
        }
    }
#endif

    if (args->result < 0) {
        errno = args->lerrno;
        switch (errno) {
            case EINTR:
#ifdef ERESTART
            case ERESTART:
#endif
#ifndef HAVE_RB_THREAD_BLOCKING_REGION
                rb_thread_schedule();
#endif
                goto retry;
        }
        rb_sys_fail("poll");
        return Qnil;
    }

    if (next_time) {
        now = gethrtime();
    }

    if (args->result > 0) {
        uint32_t cnt = args->result;
        uint32_t fd_n = 0, ev_n = 0;
        while (cnt && fd_n < args->nfd && ev_n < loop->events.count) {
            struct pollfd *res = args->fds + fd_n;
            rb_mt_socket_list *list = loop->events.sockets + ev_n;
            rb_mt_event *sock = list->first;

            /* if plugin used correctly, this checks are noop */
            if (res->fd < list->socket) {
                fd_n++;
                continue;
            } else if (res->fd > list->socket) {
                ev_n++;
                continue;
            }

            if (res->revents) {
                short flags =
                    ((res->revents & POLLIN_SET) ? LCB_READ_EVENT : 0) |
                    ((res->revents & POLLOUT_SET) ? LCB_WRITE_EVENT : 0);
                cnt--;
                loop_enque_events(&loop->callbacks, sock, flags);
            }
            fd_n++;
            ev_n++;
        }
        callbacks_run(&loop->callbacks);
    }

    if (next_time) {
        timers_run(&loop->timers, now);
    }
    if (loop->events.count == 0 && loop->timers.count == 0) {
        loop->run = 0;
    }
    return Qnil;
}

    static VALUE
loop_poll_cleanup(VALUE argp)
{
    lp_arg *args = DATA_PTR(argp);
    if (args) {
        callbacks_clean(&args->loop->callbacks);
        lp_arg_free(args);
        DATA_PTR(argp) = 0;
    }
    return Qnil;
}
#endif
/* loop poll implementation end */

    static void
loop_run(rb_mt_loop *loop)
{

    loop->run = 1;

    while(loop->run) {
#ifdef HAVE_POLL
        /* prefer use of poll when it gives some benefits, but use rb_thread_fd_select when it is sufficient */
        lcb_socket_t max = events_max_fd(&loop->events);
        int use_poll = max >= 128;
        if (use_poll) {
            lp_arg *args;
            VALUE argp = lp_arg_alloc(&args);
            args->loop = loop;
            rb_ensure(loop_run_poll, (VALUE)args, loop_poll_cleanup, argp);
        } else
#endif
        {
            ls_arg *args;
            VALUE argp = ls_arg_alloc(&args);
            args->loop = loop;
            rb_ensure(loop_run_select, (VALUE)args, loop_select_cleanup, argp);
        }
    }
}

    static void *
lcb_io_create_event(struct lcb_io_opt_st *iops)
{
    rb_mt_event *event = calloc(1, sizeof(*event));
    (void)iops;
    event->loop_index = -1;
    return event;
}

    static int
lcb_io_update_event(struct lcb_io_opt_st *iops,
        lcb_socket_t sock,
        void *eventp,
        short flags,
        void *cb_data,
        void (*handler)(lcb_socket_t sock,
            short which,
            void *cb_data))
{
    rb_mt_loop *loop = iops->v.v0.cookie;
    rb_mt_event *event = eventp;
    short old_flags = event->flags;

    if (event->inserted && old_flags == flags &&
            cb_data == event->cb_data && handler == event->handler)
    {
        return 0;
    }
    loop_remove_event(loop, event);
    event->flags = flags;
    event->cb_data = cb_data;
    event->handler = handler;
    event->socket = sock;
    if (!event->inserted) {
        events_insert(&loop->events, event);
    }
    if ((old_flags & flags) != old_flags) {
        events_fix_flags(&loop->events, sock);
    }
    return 0;
}

    static void
lcb_io_delete_event(struct lcb_io_opt_st *iops,
        lcb_socket_t sock,
        void *event)
{
    loop_remove_event((rb_mt_loop*)iops->v.v0.cookie, (rb_mt_event*)event);
    (void)sock;
}

    static void
lcb_io_destroy_event(struct lcb_io_opt_st *iops,
        void *event)
{
    lcb_io_delete_event(iops, -1, event);
    free(event);
}

    static void *
lcb_io_create_timer(struct lcb_io_opt_st *iops)
{
    rb_mt_timer *timer = calloc(1, sizeof(*timer));
    timer->index = -1;
    (void)iops;
    return timer;
}

    static int
lcb_io_update_timer(struct lcb_io_opt_st *iops, void *event,
        lcb_uint32_t usec, void *cb_data,
        void (*handler)(lcb_socket_t sock, short which, void *cb_data))
{
    rb_mt_loop *loop = iops->v.v0.cookie;
    rb_mt_timer *timer = event;

    timer->period = usec * (hrtime_t)1000;
    timer->ts = gethrtime() + timer->period;
    timer->cb_data = cb_data;
    timer->handler = handler;
    if (timer->index != -1) {
        timers_heapify_item(&loop->timers, timer->index);
    } else {
        timers_insert(&loop->timers, timer);
    }
    return 0;
}

    static void
lcb_io_delete_timer(struct lcb_io_opt_st *iops, void *event)
{
    rb_mt_loop *loop = iops->v.v0.cookie;
    rb_mt_timer *timer = event;
    if (timer->index != -1) {
        timers_remove_timer(&loop->timers, timer);
    }
}

    static void
lcb_io_destroy_timer(struct lcb_io_opt_st *iops, void *timer)
{
    lcb_io_delete_timer(iops, timer);
    free(timer);
}

    static void
lcb_io_stop_event_loop(struct lcb_io_opt_st *iops)
{
    rb_mt_loop *loop = iops->v.v0.cookie;
    loop->run = 0;
}

    static void
lcb_io_run_event_loop(struct lcb_io_opt_st *iops)
{
    rb_mt_loop *loop = iops->v.v0.cookie;
    loop_run(loop);
}

    static void
lcb_destroy_io_opts(struct lcb_io_opt_st *iops)
{
    rb_mt_loop *loop = iops->v.v0.cookie;
    loop_destroy(loop);
    free(iops);
}

    LIBCOUCHBASE_API lcb_error_t
cb_create_ruby_mt_io_opts(int version, lcb_io_opt_t *io, void *arg)
{
    struct lcb_io_opt_st *ret;
    rb_mt_loop *loop;
    (void)arg;
    if (version != 0) {
        return LCB_PLUGIN_VERSION_MISMATCH;
    }
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
    ret->v.v0.recv = cb_io_recv;
    ret->v.v0.send = cb_io_send;
    ret->v.v0.recvv = cb_io_recvv;
    ret->v.v0.sendv = cb_io_sendv;
    ret->v.v0.socket = cb_io_socket;
    ret->v.v0.close = cb_io_close;
    ret->v.v0.connect = cb_io_connect;
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

    loop = loop_create();
    if (loop == NULL) {
        free(ret);
        return LCB_CLIENT_ENOMEM;
    }
    ret->v.v0.cookie = loop;
    *io = ret;
    return LCB_SUCCESS;
}
#endif /* _WIN32 */
