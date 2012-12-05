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
#include "kvr_timed_array.hh"
#include <string.h>

kvr_timed_array *
kvr_timed_array::update(const change_t &c, kvtimestamp_t ts, threadinfo &ti) const
{
    assert(ts >= ts_);
    kvr_timed_array *row = make_sized_row(std::max(count_columns(c), (int)ncol_),
                                          ts, ti);
    // make a shallow copy
    memcpy(row->cols_, cols_, ncol_ * sizeof(cols_[0]));
    // update it
    row->update(c, ti);
    return row;
}

void
kvr_timed_array::update(const change_t &c, threadinfo &ti)
{
    change_t::const_iterator cb = c.begin(), ce = c.end();
    for (; cb != ce; ++cb)
	cols_[cb->c_fid] = inline_string::allocate(cb->c_value, ti);
}

void
kvr_timed_array::filteremit(const fields_t &f, query<kvr_timed_array> &,
			    struct kvout *kvout) const
{
    if (f.size() == 0) {
        KVW(kvout, ncol_);
        for (short i = 0; i < ncol_; i++)
            kvwrite_inline_string(kvout, cols_[i]);
    } else {
        short n = f.size();
        KVW(kvout, n);
        for (short i = 0; i < n; i++)
            kvwrite_inline_string(kvout, cols_[f[i]]);
    }
}

kvr_timed_array *
kvr_timed_array::make_sized_row(int ncol, kvtimestamp_t ts, threadinfo &ti)
{
    kvr_timed_array *tv;
    size_t len = sizeof(*tv) + ncol * sizeof(tv->cols_[0]);
    tv = (kvr_timed_array *) ti.allocate(len, memtag_row_array);
    tv->ncol_ = ncol;
    memset(tv->cols_, 0, sizeof(tv->cols_[0]) * ncol);
    tv->ts_ = ts;
    return tv;
}

void
kvr_timed_array::to_shared_row_str(str &val, kvout *buffer) const
{
    kvout_reset(buffer);
    KVW(buffer, ncol_);
    for (short i = 0; i < ncol_; i++)
        kvwrite_inline_string(buffer, cols_[i]);
    val.assign(buffer->buf, buffer->n);
}

kvr_timed_array *
kvr_timed_array::from_rowstr(str rstr, kvtimestamp_t ts, threadinfo &ti)
{
    struct kvin kvin;
    kvin_init(&kvin, const_cast<char *>(rstr.s), rstr.len);
    short ncol;
    KVR(&kvin, ncol);
    kvr_timed_array *row = make_sized_row(ncol, ts, ti);
    for (short i = 0; i < ncol; i++)
        row->cols_[i] = inline_string::allocate_read(&kvin, ti);
    return row;
}

kvr_timed_array *
kvr_timed_array::from_change(const change_t &c, kvtimestamp_t ts, threadinfo &ti)
{
    kvr_timed_array *row = make_sized_row(count_columns(c), ts, ti);
    row->update(c, ti);
    return row;
}

void
kvr_timed_array::deallocate(threadinfo &ti)
{
    for (short i = 0; i < ncol_; ++i)
        if (cols_[i])
	    cols_[i]->deallocate(ti);
    ti.deallocate(this, shallow_size(), memtag_row_array);
}

void
kvr_timed_array::deallocate_rcu(threadinfo &ti)
{
    for (short i = 0; i < ncol_; ++i)
        if (cols_[i])
	    cols_[i]->deallocate_rcu(ti);
    ti.deallocate_rcu(this, shallow_size(), memtag_row_array);
}

void
kvr_timed_array::deallocate_rcu_after_update(const change_t &c, threadinfo &ti)
{
    change_t::const_iterator cb = c.begin(), ce = c.end();
    for (; cb != ce && cb->c_fid < ncol_; ++cb)
	if (cols_[cb->c_fid])
	    cols_[cb->c_fid]->deallocate_rcu(ti);
    ti.deallocate_rcu(this, shallow_size(), memtag_row_array);
}

void
kvr_timed_array::deallocate_after_failed_update(const change_t &c, threadinfo &ti)
{
    change_t::const_iterator cb = c.begin(), ce = c.end();
    for (; cb != ce; ++cb)
	if (cols_[cb->c_fid])
	    cols_[cb->c_fid]->deallocate(ti);
    ti.deallocate(this, shallow_size(), memtag_row_array);
}
