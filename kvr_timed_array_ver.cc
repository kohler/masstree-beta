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
#include "kvr_timed_array_ver.hh"
#include <string.h>

kvr_timed_array_ver *
kvr_timed_array_ver::update(const change_t &c, kvtimestamp_t ts, threadinfo &ti)
{
    assert(ts >= ts_);
    int ncol = count_columns(c);
    if (ncol > ncol_) {
	kvr_timed_array_ver *r = make_sized_row(ncol, ts, ti);
	memcpy(r->cols_, cols_, sizeof(r->cols_[0]) * ncol_);
	r->update(c, ti);
	return r;
    } else {
	ts_ = ts;
	update(c, ti);
	return this;
    }
}

void
kvr_timed_array_ver::update(const change_t &c, threadinfo &ti)
{
    ver_.setdirty();
    // show dirty bit before any change to columns
    fence();
    change_t::const_iterator cb = c.begin(), ce = c.end();
    for (; cb != ce; ++cb) {
	if (cols_[cb->c_fid])
	    cols_[cb->c_fid]->deallocate_rcu(ti);
	cols_[cb->c_fid] = inline_string::allocate(cb->c_value, ti);
    }
    fence();
    ver_.clearandbump();
}

void
kvr_timed_array_ver::filteremit(const fields_t &f, query<kvr_timed_array_ver> &q, struct kvout *kvout) const
{
    KUtil::vec<void *> &snap = q.helper_.snapshot;
    const short n = f.size();
    // take a snapshot of columns
    rowversion v1 = ver_.stable();
    snap.resize(ncol_);
    while (1) {
        if (!n) {
            memcpy(&snap[0], cols_, sizeof(void *) * ncol_);
        } else {
            for (short i = 0; i < n; i++)
                snap[f[i]] = cols_[i];
        }
        rowversion v2 = ver_.stable();
        if (!v2.has_changed(v1))
            break;
        v1 = v2;
    }
    inline_string **snapcols = (inline_string **)&snap[0];
    if (!n) {
        for (short i = 0; i < ncol_; i++)
            prefetch(snapcols[i]);
        KVW(kvout, ncol_);
        for (short i = 0; i < ncol_; i++)
            kvwrite_inline_string(kvout, snapcols[i]);
    } else {
        for (short i = 0; i < n; i++)
            prefetch(snapcols[f[i]]);
        KVW(kvout, n);
        for (short i = 0; i < n; i++)
            kvwrite_inline_string(kvout, snapcols[f[i]]);
    }
}

kvr_timed_array_ver *
kvr_timed_array_ver::make_sized_row(int ncol, kvtimestamp_t ts, threadinfo &ti) {
    kvr_timed_array_ver *tv;
    size_t len = sizeof(*tv) + ncol * sizeof(tv->cols_[0]);
    tv = (kvr_timed_array_ver *) ti.allocate(len, memtag_row_array_ver);
    tv->ncol_ = ncol;
    tv->ver_ = rowversion();
    memset(tv->cols_, 0, sizeof(tv->cols_[0]) * ncol);
    tv->ts_ = ts;
    return tv;
}

void
kvr_timed_array_ver::to_shared_row_str(str &val, kvout *buffer) const
{
    kvout_reset(buffer);
    KVW(buffer, ncol_);
    for (short i = 0; i < ncol_; i++)
        kvwrite_inline_string(buffer, cols_[i]);
    val.assign(buffer->buf, buffer->n);
}

kvr_timed_array_ver *
kvr_timed_array_ver::from_rowstr(str rstr, kvtimestamp_t ts, threadinfo &ti)
{
    struct kvin kvin;
    kvin_init(&kvin, const_cast<char *>(rstr.s), rstr.len);
    short ncol;
    KVR(&kvin, ncol);
    kvr_timed_array_ver *row = make_sized_row(ncol, ts, ti);
    for (short i = 0; i < ncol; i++)
        row->cols_[i] = inline_string::allocate_read(&kvin, ti);
    return row;
}

kvr_timed_array_ver *
kvr_timed_array_ver::from_change(const change_t &c, kvtimestamp_t ts, threadinfo &ti)
{
    kvr_timed_array_ver *row = make_sized_row(count_columns(c), ts, ti);
    row->update(c, ti);
    return row;
}

void
kvr_timed_array_ver::deallocate(threadinfo &ti)
{
    for (short i = 0; i < ncol_; ++i)
        if (cols_[i])
	    cols_[i]->deallocate(ti);
    ti.deallocate(this, shallow_size(), memtag_row_array_ver);
}

void
kvr_timed_array_ver::deallocate_rcu(threadinfo &ti)
{
    for (short i = 0; i < ncol_; ++i)
        if (cols_[i])
	    cols_[i]->deallocate_rcu(ti);
    ti.deallocate_rcu(this, shallow_size(), memtag_row_array_ver);
}
