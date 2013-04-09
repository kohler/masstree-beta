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
#ifndef KVDB_CHECKPOINT_HH
#define KVDB_CHECKPOINT_HH 1
#include "kvrow.hh"
#include "kvio.hh"

enum { CkpKeyPrefixLen = 8 };

struct ckstate {
    kvout *keys; // array of first CkpKeyPrefixLen bytes of each key
    kvout *vals; // key \0 val \0
    kvout *ind; // array of indices into vals
    uint64_t count; // total nodes written
    uint64_t bytes;
    pthread_cond_t state_cond;
    volatile int state;
    threadinfo *ti;
    query<row_type> q;
};

#endif
