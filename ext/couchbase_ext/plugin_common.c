#include "couchbase_ext.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>

#ifndef RUBY_WIN32_H
#  include <unistd.h>
#ifdef HAVE_FCNTL_H
#  include <fcntl.h>
#endif
#define INVALID_SOCKET (-1)
#else /* RUBY_WIN32_h */
static st_table *socket_2_fd = NULL;
#endif

/* Copied from libev plugin */
    lcb_ssize_t
cb_io_recv(struct lcb_io_opt_st *iops, lcb_socket_t sock,
        void *buffer, lcb_size_t len, int flags)
{
    lcb_ssize_t ret = recv(sock, buffer, len, flags);
    if (ret < 0) {
        iops->v.v0.error = errno;
    }
    return ret;
}

    lcb_ssize_t
cb_io_recvv(struct lcb_io_opt_st *iops, lcb_socket_t sock,
        struct lcb_iovec_st *iov, lcb_size_t niov)
{
    struct msghdr msg;
    struct iovec vec[2];
    lcb_ssize_t ret;

    if (niov != 2) {
        return -1;
    }
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = vec;
    msg.msg_iovlen = iov[1].iov_len ? (lcb_size_t)2 : (lcb_size_t)1;
    msg.msg_iov[0].iov_base = iov[0].iov_base;
    msg.msg_iov[0].iov_len = iov[0].iov_len;
    msg.msg_iov[1].iov_base = iov[1].iov_base;
    msg.msg_iov[1].iov_len = iov[1].iov_len;
    ret = recvmsg(sock, &msg, 0);

    if (ret < 0) {
        iops->v.v0.error = errno;
    }

    return ret;
}

    lcb_ssize_t
cb_io_send(struct lcb_io_opt_st *iops, lcb_socket_t sock,
        const void *msg, lcb_size_t len, int flags)
{
    lcb_ssize_t ret = send(sock, msg, len, flags);
    if (ret < 0) {
        iops->v.v0.error = errno;
    }
    return ret;
}

    lcb_ssize_t
cb_io_sendv(struct lcb_io_opt_st *iops, lcb_socket_t sock,
        struct lcb_iovec_st *iov, lcb_size_t niov)
{
    struct msghdr msg;
    struct iovec vec[2];
    lcb_ssize_t ret;

    if (niov != 2) {
        return -1;
    }
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = vec;
    msg.msg_iovlen = iov[1].iov_len ? (lcb_size_t)2 : (lcb_size_t)1;
    msg.msg_iov[0].iov_base = iov[0].iov_base;
    msg.msg_iov[0].iov_len = iov[0].iov_len;
    msg.msg_iov[1].iov_base = iov[1].iov_base;
    msg.msg_iov[1].iov_len = iov[1].iov_len;
    ret = sendmsg(sock, &msg, 0);

    if (ret < 0) {
        iops->v.v0.error = errno;
    }
    return ret;
}

    static int
make_socket_nonblocking(lcb_socket_t sock)
{
    int flags = 0;
#ifdef F_GETFL
    if ((flags = fcntl(sock, F_GETFL, NULL)) < 0) {
        return -1;
    }
#endif
    if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1) {
        return -1;
    }

    return 0;
}

    static int
close_socket(lcb_socket_t sock)
{
    return close(sock);
}

    lcb_socket_t
cb_io_socket(struct lcb_io_opt_st *iops, int domain, int type,
        int protocol)
{
    lcb_socket_t sock = socket(domain, type, protocol);
    if (sock == INVALID_SOCKET) {
        iops->v.v0.error = errno;
    } else {
        if (make_socket_nonblocking(sock) != 0) {
            int error = errno;
            iops->v.v0.close(iops, sock);
            iops->v.v0.error = error;
            sock = INVALID_SOCKET;
        }
    }

    return sock;
}

    void
cb_io_close(struct lcb_io_opt_st *iops, lcb_socket_t sock)
{
    close_socket(sock);
    (void)iops;
}

    int
cb_io_connect(struct lcb_io_opt_st *iops, lcb_socket_t sock,
        const struct sockaddr *name, unsigned int namelen)
{
    int ret = connect(sock, name, (socklen_t)namelen);
    if (ret < 0) {
        iops->v.v0.error = errno;
    }
    return ret;
}

