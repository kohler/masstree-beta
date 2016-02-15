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
#include "memdebug.hh"
#include <stdio.h>
#include <assert.h>

#if HAVE_MEMDEBUG
void memdebug::landmark(char* buf, size_t size) const {
    if (this->magic != magic_value && this->magic != magic_free_value)
        snprintf(buf, size, "???");
    else if (this->file)
        snprintf(buf, size, "%s:%d", this->file, this->line);
    else if (this->line)
        snprintf(buf, size, "%d", this->line);
    else
        snprintf(buf, size, "0");
}

void
memdebug::hard_free_checks(const memdebug *m, size_t size, int freetype,
                           int after_rcu, const char *op) {
    char buf[256];
    m->landmark(buf, sizeof(buf));
    if (m->magic == magic_free_value)
        fprintf(stderr, "%s(%p): double free, was @%s\n",
                op, m + 1, buf);
    else if (m->magic != magic_value)
        fprintf(stderr, "%s(%p): freeing unallocated pointer (%x)\n",
                op, m + 1, m->magic);
    assert(m->magic == magic_value);
    if (freetype && m->freetype != freetype)
        fprintf(stderr, "%s(%p): expected type %x, saw %x, "
                "allocated %s\n", op, m + 1, freetype, m->freetype, buf);
    if (!after_rcu && m->size != size)
        fprintf(stderr, "%s(%p): expected size %lu, saw %lu, "
                "allocated %s\n", op, m + 1,
                (unsigned long) size, (unsigned long) m->size, buf);
    if (m->after_rcu != after_rcu)
        fprintf(stderr, "%s(%p): double free after rcu, allocated @%s\n",
                op, m + 1, buf);
    if (freetype)
        assert(m->freetype == freetype);
    if (!after_rcu)
        assert(m->size == size);
    assert(m->after_rcu == after_rcu);
}

void
memdebug::hard_assert_use(const void *ptr, memtag tag1, memtag tag2) {
    const memdebug *m = reinterpret_cast<const memdebug *>(ptr) - 1;
    char tagbuf[40], buf[256];
    m->landmark(buf, sizeof(buf));
    if (tag2 == (memtag) -1)
        sprintf(buf, "%x", tag1);
    else
        sprintf(buf, "%x/%x", tag1, tag2);
    if (m->magic == magic_free_value)
        fprintf(stderr, "%p: use tag %s after free, allocated %s\n",
                m + 1, tagbuf, buf);
    else if (m->magic != magic_value)
        fprintf(stderr, "%p: pointer is unallocated, not tag %s\n",
                m + 1, tagbuf);
    assert(m->magic == magic_value);
    if (tag1 != 0 && (m->freetype >> 8) != tag1 && (m->freetype >> 8) != tag2)
        fprintf(stderr, "%p: expected tag %s, got tag %x, allocated %s\n",
                m + 1, tagbuf, m->freetype >> 8, buf);
    if (tag1 != 0)
        assert((m->freetype >> 8) == tag1 || (m->freetype >> 8) == tag2);
}
#endif
