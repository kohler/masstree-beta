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
#ifndef KVIO_H
#define KVIO_H

#include "misc.hh"
#include "shared_config.hh"
#include <string>
#include <vector>
#include <stdlib.h>

struct kvin {
    int fd;
    char* buf;
    int len; // underlying size of buf[]
    // buf[i0..i1-1] are valid
    int i0;
    int i1;
};

kvin* new_kvin(int fd, int buflen);
kvin* new_bufkvin(char* buf);
void kvin_init(kvin* kv, char* buf, int len);
void kvin_setlen(kvin* kv, int len);
char* kvin_skip(kvin* kv, int n);
void free_kvin(kvin* kv);
int kvread(kvin* kv, char* buf, int n);
int kvcheck(kvin* kv, int tryhard);
int mayblock_kvoneread(kvin* kv);

struct kvout {
    int fd;
    char* buf;
    uint64_t len; // allocated size of buf
    uint64_t n;   // # of chars we've written to buf
};

kvout* new_kvout(int fd, int buflen);
kvout* new_bufkvout();
void kvout_reset(kvout* kv);
void free_kvout(kvout* kv);
int kvwrite(kvout* kv, const void* buf, unsigned int n);
void kvflush(kvout* kv);

template <typename T>
inline int KVR(kvin* kv, T& x) {
    return kvread(kv, (char*) &x, sizeof(T));
}

template <typename T>
inline int KVW(kvout* kv, const T& x) {
    if (kv->len - kv->n >= (int) sizeof(x)) {
        *(T*) (kv->buf + kv->n) = x;
        kv->n += sizeof(x);
    } else
        kvwrite(kv, &x, sizeof(x));
    return sizeof(x);
}

template <typename T>
inline int KVW(kvout* kv, const volatile T& x) {
    return KVW(kv, (const T &)x);
}

inline int KVW(kvout* kv, Str x) {
    KVW(kv, int32_t(x.length()));
    kvwrite(kv, x.data(), x.length());
    return sizeof(int32_t) + x.length();
}

inline int kvread_str_inplace(kvin* kv, Str& v) {
    KVR(kv, v.len);
    v.s = kvin_skip(kv, v.len);
    return sizeof(v.len) + v.len;
}

inline int kvread_str_alloc(kvin* kv, Str& v) {
    KVR(kv, v.len);
    char *buf = (char *)malloc(v.len);
    kvread(kv, buf, v.len);
    v.s = buf;
    return sizeof(v.len) + v.len;
}

inline int kvread_str(kvin* kv, char* buf, int max, int& vlen) {
    KVR(kv, vlen);
    mandatory_assert(vlen <= max);
    kvread(kv, buf, vlen);
    return sizeof(vlen) + vlen;
}

inline int kvwrite_str(kvout* kv, Str v) {
    KVW(kv, (int)v.len);
    kvwrite(kv, v.s, v.len);
    return sizeof(v.len) + v.len;
}

inline int kvwrite_inline_string(kvout* kv, const inline_string* s) {
    if (!s)
        return KVW(kv, Str());
    else
        return KVW(kv, Str(s->s, s->len));
}

/** @brief Read a row from kvin. The row is serialized by row_type::filteremit.
 *    Ideally, kvread_row should be a member of row_base class. However, client side
 *    may prefer an STL based interface, while kvd internally does not use STL.
 *    Here we provide an STL based interface for the client side.
 */
inline int kvread_row(struct kvin* kv, std::vector<std::string>& row) {
    short n;
    if (KVR(kv, n) != sizeof(n))
        return -1;
    int x, y;
    x = 0;
    for (int i = 0; i < n; i++) {
        char val[MaxRowLen];
        int vallen;
        if ((y = kvread_str(kv, val, sizeof(val), vallen)) < 0)
            return -1;
        x += y;
        row.push_back(std::string(val, vallen));
    }
    return sizeof(n) + x;
}

template <typename ALLOC>
inline inline_string* inline_string::allocate_read(kvin* kv, ALLOC& ti) {
    int len;
    int r = KVR(kv, len);
    mandatory_assert(r == sizeof(len) && len < MaxRowLen);
    inline_string* v = (inline_string*) ti.allocate(len + sizeof(inline_string));
    assert(v);
    v->len = len;
    r = kvread(kv, v->s, len);
    assert(r == len);
    return v;
}

#endif
