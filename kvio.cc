/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2013 President and Fellows of Harvard College
 * Copyright (c) 2012-2013 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
// buffered read and write for kvc/kvd.
// stdio is good but not quite what I want.
// need to be able to check if any input
// available, and do non-blocking check.
// also, fwrite just isn't very fast, at
// least on the Mac.

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/select.h>
#include <sys/time.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include "kvio.hh"

// API to allocate a new kvin.
kvin* new_kvin(int fd, int buflen) {
    kvin* kv = (kvin*) malloc(sizeof(kvin));
    assert(kv);
    memset(kv, 0, sizeof(*kv));
    kv->len = buflen;
    kv->buf = (char*) malloc(kv->len);
    assert(kv->buf);
    kv->fd = fd;
    return kv;
}

// Allocate a kvin for an existing buffer, no fd.
kvin* new_bufkvin(char* buf) {
    kvin* kv = (kvin*) malloc(sizeof(kvin));
    assert(kv);
    memset(kv, 0, sizeof(*kv));
    kv->len = 0;
    kv->buf = buf;
    kv->fd = -1;
    return kv;
}

void kvin_init(kvin* kv, char* buf, int len) {
    memset(kv, 0, sizeof(*kv));
    kv->buf = buf;
    kv->fd = -1;
    kvin_setlen(kv, len);
}

void kvin_setlen(kvin* kv, int len) {
    assert(kv->fd == -1);
    kv->len = len;
    kv->i0 = 0;
    kv->i1 = len;
}

char* kvin_skip(kvin* kv, int n) {
    assert(kv->fd == -1);
    char *p = &kv->buf[kv->i0];
    kv->i0 += n;
    assert(kv->i0 <= kv->i1);
    return p;
}

// API to free a kvin.
// does not close() the fd.
void free_kvin(kvin* kv) {
    if (kv->buf)
        free(kv->buf);
    kv->buf = 0;
    free(kv);
}

// internal.
// do a read().
static int kvreallyread(kvin* kv) {
    if (kv->i0 == kv->i1)
        kv->i0 = kv->i1 = 0;
    int wanted = kv->len - kv->i1;
    assert(kv->fd >= 0);
    assert(wanted > 0 && kv->i1 + wanted <= kv->len);
    int cc = read(kv->fd, kv->buf + kv->i1, wanted);
    if (cc < 0)
        return -1;
    kv->i1 += cc;
    return kv->i1 - kv->i0;
}

// API to read exactly n bytes.
// return n or -1.
int kvread(kvin* kv, char* buf, int n) {
    int i = 0;
    while (i < n) {
        if (kv->i1 > kv->i0){
            int cc = kv->i1 - kv->i0;
            if (cc > n - i)
                cc = n - i;
            memcpy(buf + i, kv->buf + kv->i0, cc);
            i += cc;
            kv->i0 += cc;
        } else {
            if (kvreallyread(kv) < 1)
                return -1;
        }
    }
    return n;
}

// API to see how much data is waiting.
// tryhard=0 -> just check buffer.
// tryhard=1 -> do non-blocking read if buf is empty.
// tryhard=2 -> block if buf is empty.
int kvcheck(kvin* kv, int tryhard) {
    if (kv->i1 == kv->i0 && tryhard) {
        assert(kv->fd >= 0);
        if (tryhard == 2) {
            // blocking read
            if (kvreallyread(kv) <= 0)
                return -1;
        } else if (tryhard == 1) {
            // non-blocking read
            always_assert(kv->fd < FD_SETSIZE);
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(kv->fd, &rfds);
            struct timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 0;
            select(kv->fd + 1, &rfds, NULL, NULL, &tv);
            if (FD_ISSET(kv->fd, &rfds))
                kvreallyread(kv);
        }
    }
    return kv->i1 - kv->i0;
}

// read from socket once.
// return the number of bytes available in the socket; -1 on error
int mayblock_kvoneread(kvin* kv) {
    if (kvreallyread(kv) <= 0)
        return -1;
    return kv->i1 - kv->i0;
}

// API to allocate a new kvout.
kvout* new_kvout(int fd, int buflen) {
    kvout* kv = (kvout*) malloc(sizeof(kvout));
    assert(kv);
    memset(kv, 0, sizeof(*kv));
    kv->capacity = buflen;
    kv->buf = (char*) malloc(kv->capacity);
    assert(kv->buf);
    kv->fd = fd;
    return kv;
}

// API to allocate a new kvout for a buffer, no fd.
kvout* new_bufkvout() {
    kvout *kv = (kvout*) malloc(sizeof(kvout));
    assert(kv);
    memset(kv, 0, sizeof(*kv));
    kv->capacity = 256;
    kv->buf = (char*) malloc(kv->capacity);
    assert(kv->buf);
    kv->n = 0;
    kv->fd = -1;
    return kv;
}

// API to clear out a buf kvout.
void kvout_reset(kvout* kv) {
    assert(kv->fd < 0);
    kv->n = 0;
}

// API to free a kvout.
// does not close() the fd.
void free_kvout(kvout* kv) {
    if (kv->buf)
        free(kv->buf);
    kv->buf = 0;
    free(kv);
}

void kvflush(kvout* kv) {
    assert(kv->fd >= 0);
    size_t sent = 0;
    while (kv->n > sent) {
        ssize_t cc = write(kv->fd, kv->buf + sent, kv->n - sent);
        if (cc <= 0) {
            if (errno == EWOULDBLOCK) {
                usleep(1);
                continue;
            }
            perror("kvflush write");
            return;
        }
        sent += cc;
    }
    kv->n = 0;
}

// API
void kvout::grow(unsigned want) {
    if (fd >= 0)
        kvflush(this);
    if (want == 0)
        want = capacity + 1;
    while (want > capacity)
        capacity *= 2;
    buf = (char*) realloc(buf, capacity);
    assert(buf);
}

int kvwrite(kvout* kv, const void* buf, unsigned n) {
    if (kv->n + n > kv->capacity && kv->fd >= 0)
        kvflush(kv);
    if (kv->n + n > kv->capacity)
        kv->grow(kv->n + n);
    memcpy(kv->buf + kv->n, buf, n);
    kv->n += n;
    return n;
}
