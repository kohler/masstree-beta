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
#include "kvrow.hh"
#include "kvr_timed_str.hh"
#include <string.h>

kvr_timed_str *
kvr_timed_str::update(const change_t &c, kvtimestamp_t ts, threadinfo &ti) const
{
    bool toend;
    int max = endat(c, toend);
    if (!toend)
        max = std::max(max, vallen_);
    kvr_timed_str *row = make_sized_row(max, ts, ti);
    memcpy(row->s_, s_, std::min(max, vallen_));
    row->update(c);
    return row;
}

void
kvr_timed_str::update(const change_t &c)
{
    for (unsigned i = 0; i < c.size(); i++)
        memcpy(s_ + c[i].c_fid.f_off, c[i].c_value.s, c[i].c_value.len);
}

int
kvr_timed_str::endat(const change_t &c, bool &toend)
{
    toend = false;
    int vlen = 0;
    for (unsigned i = 0; i < c.size(); i++) {
        vlen = std::max(vlen, c[i].c_fid.f_off + c[i].c_value.len);
        if (c[i].c_fid.f_len == -1)
            toend = true;
    }
    return vlen;
}

void
kvr_timed_str::filteremit(const fields_t &f, query<kvr_timed_str> &, struct kvout *kvout) const
{
    if (f.size() == 0) {
        KVW(kvout, (short)1);
        kvwrite_str(kvout, str(s_, vallen_));
    } else {
        short n = f.size();
        KVW(kvout, n);
        for (int i = 0; i < n; i++) {
            int len = (f[i].f_len == -1) ? (vallen_ - f[i].f_off) : f[i].f_len;
            assert(len > 0);
            kvwrite_str(kvout, str(s_ + f[i].f_off, len));
        }
    }
}

kvr_timed_str *
kvr_timed_str::make_sized_row(int vlen, kvtimestamp_t ts, threadinfo &ti) {
    size_t len = sizeof(kvr_timed_str) + vlen;
    kvr_timed_str *tv = (kvr_timed_str *) ti.allocate(len, memtag_row_str);
    tv->vallen_ = vlen;
    tv->ts_ = ts;
    return tv;
}

kvr_timed_str *
kvr_timed_str::from_rowstr(str rb, kvtimestamp_t ts, threadinfo &ti)
{
    kvr_timed_str *row = make_sized_row(rb.len, ts, ti);
    memcpy(row->s_, rb.s, rb.len);
    return row;
}

kvr_timed_str *
kvr_timed_str::from_change(const kvr_timed_str::change_t &c, kvtimestamp_t ts,
                           threadinfo &ti)
{
    bool toend;
    kvr_timed_str *row = make_sized_row(endat(c, toend), ts, ti);
    row->update(c);
    return row;
}
