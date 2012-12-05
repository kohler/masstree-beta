/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012 President and Fellows of Harvard College
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file is
 * legally binding.
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
  char *buf;
  int len; // underlying size of buf[]
  // buf[i0..i1-1] are valid
  int i0;
  int i1;
};

struct kvin *new_kvin(int fd, int buflen);
struct kvin *new_bufkvin(char *buf);
void kvin_init(struct kvin *kvin, char *buf, int len);
void kvin_setlen(struct kvin *, int len);
char *kvin_skip(struct kvin *, int n);
void free_kvin(struct kvin *kvin);
int kvread(struct kvin *kvin, char *buf, int n);
int kvcheck(struct kvin *kvin, int tryhard);
int mayblock_kvoneread(struct kvin *kvin);

struct kvout {
  int fd;
  char *buf;
  uint64_t len; // allocated size of buf
  uint64_t n;   // # of chars we've written to buf
};

struct kvout *new_kvout(int fd, int buflen);
struct kvout *new_bufkvout();
void kvout_reset(struct kvout *kvout);
void free_kvout(struct kvout *kvout);
int kvwrite(struct kvout *kvout, const void *buf, unsigned int n);
void kvflush(struct kvout *kvout);

template <typename T>
inline int
KVR(struct kvin *kvin, T &x)
{
  return kvread(kvin, (char *)&x, sizeof(T));
}

template <typename T>
inline int
KVW(struct kvout *kvout, const T &x)
{
  if (kvout->len - kvout->n >= (int) sizeof(x)) {
    *(T *)(kvout->buf + kvout->n) = x;
    kvout->n += sizeof(x);
  } else {
    kvwrite(kvout, &x, sizeof(x));
  }
  return sizeof(x);
}

template <typename T>
inline int
KVW(struct kvout *kvout, const volatile T &x)
{
    return KVW(kvout, (const T &)x);
}

inline int kvread_str_inplace(struct kvin *kvin, str &v)
{
    KVR(kvin, v.len);
    v.s = kvin_skip(kvin, v.len);
    return sizeof(v.len) + v.len;
}

inline int kvread_str_alloc(struct kvin *kvin, str &v)
{
    KVR(kvin, v.len);
    char *buf = (char *)malloc(v.len);
    kvread(kvin, buf, v.len);
    v.s = buf;
    return sizeof(v.len) + v.len;
}

inline int kvread_str(struct kvin *kvin, char *buf, int max, int &vlen)
{
    KVR(kvin, vlen);
    mandatory_assert(vlen <= max);
    kvread(kvin, buf, vlen);
    return sizeof(vlen) + vlen;
}

inline int kvwrite_str(struct kvout *kvout, str v)
{
    KVW(kvout, (int)v.len);
    kvwrite(kvout, v.s, v.len);
    return sizeof(v.len) + v.len;
}

inline int kvwrite_inline_string(struct kvout *kvout, inline_string *s)
{
    if (!s)
        return kvwrite_str(kvout, str(NULL, 0));
    else
        return kvwrite_str(kvout, str(s->s, s->len));
}

/** @brief Read a row from kvin. The row is serialized by row_type::filteremit.
 *    Ideally, kvread_row should be a member of row_base class. However, client side
 *    may prefer an STL based interface, while kvd internally does not use STL.
 *    Here we provide an STL based interface for the client side.
 */
inline int kvread_row(struct kvin *kvin, std::vector<std::string> &row) {
    short n;
    if(KVR(kvin, n) != sizeof(n))
        return -1;
    int x, y;
    x = 0;
    for (int i = 0; i < n; i++) {
        char val[MaxRowLen];
        int vallen;
        if ((y = kvread_str(kvin, val, sizeof(val), vallen)) < 0)
            return -1;
        x += y;
        row.push_back(std::string(val, vallen));
    }
    return sizeof(n) + x;
}

template <typename ALLOC>
inline inline_string *
inline_string::allocate_read(struct kvin *kvin, ALLOC &ti)
{
    int len;
    int r = KVR(kvin, len);
    mandatory_assert(r == sizeof(len) && len < MaxRowLen);
    inline_string *v = (inline_string *) ti.allocate(len + sizeof(inline_string));
    assert(v);
    v->len = len;
    r = kvread(kvin, v->s, len);
    assert(r == len);
    return v;
}

#endif
