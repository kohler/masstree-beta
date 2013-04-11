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
#include "kvr_timed_str.hh"
#include <string.h>

kvr_timed_str* kvr_timed_str::make_sized_row(int vlen, kvtimestamp_t ts,
                                             threadinfo& ti) {
    size_t len = sizeof(kvr_timed_str) + vlen;
    kvr_timed_str *tv = (kvr_timed_str *) ti.allocate(len, memtag_row_str);
    tv->vallen_ = vlen;
    tv->ts_ = ts;
    return tv;
}
