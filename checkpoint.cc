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
#include "checkpoint.hh"

// add one key/value to a checkpoint.
// called by checkpoint_tree() for each node.
void
checkpoint1(ckstate *c, str key, const row_type *v)
{
    int n = std::min((int)CkpKeyPrefixLen, key.len);
    kvwrite(c->keys, key.s, n);
    for (int i = n; i < CkpKeyPrefixLen; i++)
        KVW(c->keys, (char)0);
    KVW(c->ind, c->vals->n); // remember the offset of the next two
    kvwrite(c->vals, key.s, key.len);
    KVW(c->vals, (char)0);
    KVW(c->vals, v->ts_);
    str val;
    if (row_type::has_priv_row_str)
	v->to_priv_row_str(val);
    else
	v->to_shared_row_str(val, c->checkpoint_buffer_);
    kvwrite_str(c->vals, val);
    c->count += 1;
}
