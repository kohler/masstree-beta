/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2016 President and Fellows of Harvard College
 * Copyright (c) 2012-2016 Massachusetts Institute of Technology
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
#ifndef MEMDEBUG_HH
#define MEMDEBUG_HH 1
#include "mtcounters.hh"
#include <stddef.h>

struct memdebug {
#if HAVE_MEMDEBUG
    enum {
        magic_value = 389612313 /* = 0x17390319 */,
        magic_free_value = 2015593488 /* = 0x78238410 */
    };
    int magic;
    int freetype;
    size_t size;
    int after_rcu;
    int line;
    const char* file;

    static void* make(void* p, size_t size, int freetype) {
        if (p) {
            memdebug* m = reinterpret_cast<memdebug*>(p);
            m->magic = magic_value;
            m->freetype = freetype;
            m->size = size;
            m->after_rcu = 0;
            m->line = 0;
            m->file = 0;
            return m + 1;
        } else
            return p;
    }
    static void set_landmark(void* p, const char* file, int line) {
        if (p) {
            memdebug* m = reinterpret_cast<memdebug*>(p) - 1;
            m->file = file;
            m->line = line;
        }
    }
    static void* check_free(void* p, size_t size, int freetype) {
        memdebug* m = reinterpret_cast<memdebug*>(p) - 1;
        free_checks(m, size, freetype, false, "deallocate");
        m->magic = magic_free_value;
        return m;
    }
    static void check_rcu(void* p, size_t size, int freetype) {
        memdebug* m = reinterpret_cast<memdebug*>(p) - 1;
        free_checks(m, size, freetype, false, "deallocate_rcu");
        m->after_rcu = 1;
    }
    static void *check_free_after_rcu(void* p, int freetype) {
        memdebug* m = reinterpret_cast<memdebug*>(p) - 1;
        free_checks(m, 0, freetype, true, "free_after_rcu");
        m->magic = magic_free_value;
        return m;
    }
    static bool check_use(const void* p, int type) {
        const memdebug* m = reinterpret_cast<const memdebug*>(p) - 1;
        return m->magic == magic_value && (type == 0 || (m->freetype >> 8) == type);
    }
    static bool check_use(const void* p, int type1, int type2) {
        const memdebug* m = reinterpret_cast<const memdebug*>(p) - 1;
        return m->magic == magic_value
            && ((m->freetype >> 8) == type1 || (m->freetype >> 8) == type2);
    }
    static void assert_use(const void* p, memtag tag) {
        if (!check_use(p, tag))
            hard_assert_use(p, tag, (memtag) -1);
    }
    static void assert_use(const void *p, memtag tag1, memtag tag2) {
        if (!check_use(p, tag1, tag2))
            hard_assert_use(p, tag1, tag2);
    }
  private:
    static void free_checks(const memdebug *m, size_t size, int freetype,
                            int after_rcu, const char *op) {
        if (m->magic != magic_value
            || m->freetype != freetype
            || (!after_rcu && m->size != size)
            || m->after_rcu != after_rcu)
            hard_free_checks(m, freetype, size, after_rcu, op);
    }
    void landmark(char* buf, size_t size) const;
    static void hard_free_checks(const memdebug* m, size_t size, int freetype,
                                 int after_rcu, const char* op);
    static void hard_assert_use(const void* ptr, memtag tag1, memtag tag2);
#else
    static void* make(void* p, size_t, int) {
        return p;
    }
    static void set_landmark(void*, const char*, int) {
    }
    static void* check_free(void* p, size_t, int) {
        return p;
    }
    static void check_rcu(void*, size_t, int) {
    }
    static void *check_free_after_rcu(void* p, int) {
        return p;
    }
    static bool check_use(void*, memtag) {
        return true;
    }
    static bool check_use(void*, memtag, memtag) {
        return true;
    }
    static void assert_use(void*, memtag) {
    }
    static void assert_use(void*, memtag, memtag) {
    }
#endif
};

enum {
#if HAVE_MEMDEBUG
    memdebug_size = sizeof(memdebug)
#else
    memdebug_size = 0
#endif
};

#endif
