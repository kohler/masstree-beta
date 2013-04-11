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
#include "kvrow.hh"
#include "value_versioned_array.hh"
#include <string.h>

value_versioned_array* value_versioned_array::make_sized_row(int ncol, kvtimestamp_t ts, threadinfo& ti) {
    value_versioned_array* row = (value_versioned_array*) ti.allocate(shallow_size(ncol), memtag_row_array_ver);
    row->ts_ = ts;
    row->ver_ = rowversion();
    row->ncol_ = row->ncol_cap_ = ncol;
    memset(row->cols_, 0, sizeof(row->cols_[0]) * ncol);
    return row;
}

void value_versioned_array::snapshot(value_versioned_array*& storage,
                                   const fields_t& f, threadinfo& ti) const {
    if (!storage || storage->ncol_cap_ < ncol_) {
        if (storage)
            storage->deallocate(ti);
        storage = make_sized_row(ncol_, ts_, ti);
    }
    storage->ncol_ = ncol_;
    rowversion v1 = ver_.stable();
    while (1) {
        if (f.size() == 1)
            storage->cols_[f[0]] = cols_[f[0]];
        else
            memcpy(storage->cols_, cols_, sizeof(cols_[0]) * storage->ncol_);
        rowversion v2 = ver_.stable();
        if (!v1.has_changed(v2))
            break;
        v1 = v2;
    }
}

value_versioned_array*
value_versioned_array::checkpoint_read(Str str, kvtimestamp_t ts,
                                       threadinfo& ti) {
    kvin kv;
    kvin_init(&kv, const_cast<char*>(str.s), str.len);
    short ncol;
    KVR(&kv, ncol);
    value_versioned_array* row = make_sized_row(ncol, ts, ti);
    for (short i = 0; i < ncol; i++)
        row->cols_[i] = inline_string::allocate_read(&kv, ti);
    return row;
}

void value_versioned_array::checkpoint_write(kvout* kv) const {
    int sz = sizeof(ncol_);
    for (short i = 0; i != ncol_; ++i)
        sz += sizeof(int) + (cols_[i] ? cols_[i]->length() : 0);
    KVW(kv, sz);
    KVW(kv, ncol_);
    for (short i = 0; i != ncol_; i++)
        kvwrite_inline_string(kv, cols_[i]);
}

void value_versioned_array::deallocate(threadinfo &ti) {
    for (short i = 0; i < ncol_; ++i)
        if (cols_[i])
	    cols_[i]->deallocate(ti);
    ti.deallocate(this, shallow_size(), memtag_row_array_ver);
}

void value_versioned_array::deallocate_rcu(threadinfo &ti) {
    for (short i = 0; i < ncol_; ++i)
        if (cols_[i])
	    cols_[i]->deallocate_rcu(ti);
    ti.deallocate_rcu(this, shallow_size(), memtag_row_array_ver);
}
