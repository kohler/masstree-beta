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
#include <string>
#include <vector>
#include <stdlib.h>
#include "string.hh"
#include "str.hh"

struct kvin {
    int fd;
    char* buf;
    int len; // underlying size of buf[]
    // buf[i0..i1-1] are valid
    int i0;
    int i1;

    inline bool empty() const {
        return i0 == i1;
    }
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
    unsigned capacity; // allocated size of buf
    unsigned n;   // # of chars we've written to buf

    inline void append(char c);
    inline char* reserve(int n);
    inline void adjust_length(int delta);
    inline void set_end(char* end);
    void grow(unsigned want);
};

kvout* new_kvout(int fd, int buflen);
kvout* new_bufkvout();
void kvout_reset(kvout* kv);
void free_kvout(kvout* kv);
int kvwrite(kvout* kv, const void* buf, unsigned int n);
void kvflush(kvout* kv);

inline void kvout::append(char c) {
    if (n == capacity)
        grow(0);
    buf[n] = c;
    ++n;
}

inline char* kvout::reserve(int nchars) {
    if (n + nchars > capacity)
        grow(n + nchars);
    return buf + n;
}

inline void kvout::adjust_length(int delta) {
    masstree_precondition(n + delta <= capacity);
    n += delta;
}

inline void kvout::set_end(char* x) {
    masstree_precondition(x >= buf && x <= buf + capacity);
    n = x - buf;
}

#endif
