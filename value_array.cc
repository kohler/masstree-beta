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
#include "value_array.hh"
#include <string.h>

value_array* value_array::make_sized_row(int ncol, kvtimestamp_t ts,
                                         threadinfo& ti) {
    value_array *tv;
    tv = (value_array *) ti.allocate(shallow_size(ncol), memtag_row_array);
    tv->ts_ = ts;
    tv->ncol_ = ncol;
    memset(tv->cols_, 0, sizeof(tv->cols_[0]) * ncol);
    return tv;
}

value_array* value_array::checkpoint_read(Str str, kvtimestamp_t ts,
                                                  threadinfo& ti) {
    kvin kv;
    kvin_init(&kv, const_cast<char*>(str.s), str.len);
    short ncol;
    KVR(&kv, ncol);
    value_array* row = make_sized_row(ncol, ts, ti);
    for (short i = 0; i < ncol; i++)
        row->cols_[i] = inline_string::allocate_read(&kv, ti);
    return row;
}

void value_array::checkpoint_write(kvout* kv) const {
    int sz = sizeof(ncol_);
    for (short i = 0; i != ncol_; ++i)
        sz += sizeof(int) + (cols_[i] ? cols_[i]->length() : 0);
    KVW(kv, sz);
    KVW(kv, ncol_);
    for (short i = 0; i != ncol_; i++)
        kvwrite_inline_string(kv, cols_[i]);
}

void value_array::deallocate(threadinfo& ti) {
    for (short i = 0; i < ncol_; ++i)
        if (cols_[i])
	    cols_[i]->deallocate(ti);
    ti.deallocate(this, shallow_size(), memtag_row_array);
}

void value_array::deallocate_rcu(threadinfo& ti) {
    for (short i = 0; i < ncol_; ++i)
        if (cols_[i])
	    cols_[i]->deallocate_rcu(ti);
    ti.deallocate_rcu(this, shallow_size(), memtag_row_array);
}
