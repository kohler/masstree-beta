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
#ifndef PERF_STAT_HH
#define PERF_STAT_HH 1
#include "compiler.hh"
#include "misc.hh"
#include "kvdconfig.hh"
#include <stdlib.h>
#include <inttypes.h>

namespace Perf {
struct stat {
    /** @brief An initialization call from main function
     */
    static void initmain(bool pinthreads);
#if MEMSTATS
    enum { n_tracked_alloc = 2 };
    ssize_t tree_mem[n_tracked_alloc];
    size_t tree_keys;
#else
    enum { n_tracked_alloc = 0 };
#endif
#if GCSTATS
    int gc_nfree;
    int gc_nalloc;
#endif
#if GETSTATS
    bool getting;
    uint64_t nprobe;
    uint64_t ngets;
    uint64_t ntsc;      // sum of tsc
    uint64_t tsc_start;
#if PMC_ENABLED
    // pmc profiling.
    // Program pmc using amd10h_pmc - Linux profiling kernel module
    // written by Silas:
    // git+ssh://amsterdam.csail.mit.edu/home/am1/prof/proftools.git
    uint64_t pmc_lookup[4];     // accumulates events happened between mark_get_begin
                                // and mark_get_end
    uint64_t pmc_start[4];
    uint64_t pmc_firstget[4];
    double   t0_firstget;
    double   t1_lastget;
#endif
#endif
#if !NDEBUG
    uint64_t deltas_created;
    uint64_t deltas_removed;
#endif
    void initialize(int cid) {
        this->cid = cid;
    }
    void mark_gc_object_freed() {
#if GC_STATS
        gc_nfree ++;
#endif
    }
    void mark_gc_alloc(int nstub) {
        (void) nstub;
#if GC_STATS
        gc_nalloc += nstub;
#endif
    }
    void mark_tree_key() {
#if MEMSTATS
        tree_keys++;
#endif
    }
    void mark_alloc(size_t x, int alloc) {
        (void) x, (void) alloc;
#if MEMSTATS
	if (unsigned(alloc) < unsigned(n_tracked_alloc))
	    tree_mem[alloc] += x;
#endif
    }
    void mark_free(size_t x, int alloc) {
        (void) x, (void) alloc;
#if MEMSTATS
	if (unsigned(alloc) < unsigned(n_tracked_alloc))
	    tree_mem[alloc] -= x;
#endif
    }
    void mark_get_begin() {
#if GETSTATS
#if PMC_ENABLED
        for (int i = 0; i < 4; i++)
            pmc_start[i] = read_pmc(i);
        if (ngets == 0) {
            t0_firstget = now();
            memcpy(pmc_firstget, pmc_start, sizeof(pmc_start));
        }
#endif
        getting = true;
        tsc_start = read_tsc();
#endif
    }
    void mark_get_end() {
#if GETSTATS
        ntsc += read_tsc() - tsc_start;
#if PMC_ENABLED
        for (int i = 0; i < 4; i++)
            pmc_lookup[i] += read_pmc(i) - pmc_start[i];
        t1_lastget = now();
#endif
        ngets ++;
        getting = false;
#endif
    }
    void mark_hash_probe() {
#if GETSTATS
        nprobe ++;
#endif
    }
    void mark_delta_created() {
#if !NDEBUG
	++deltas_created;
#endif
    }
    void mark_delta_removed() {
#if !NDEBUG
	++deltas_removed;
#endif
    }
    static void print(const stat **s, int n);
    int cid;    // core index
};
}
#endif
