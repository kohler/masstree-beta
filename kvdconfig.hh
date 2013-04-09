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
#ifndef KVDCONFIG_HH
#define KVDCONFIG_HH 1
#include "shared_config.hh"

enum { MaxCores = 48 };   // Maximum number of cores kvdb support
enum { MaxNumaNode = 8 }; // Maximum number of Numa node kvdb statistics support
enum { CacheLineSize = 64 };
enum { BadKeyRestLen = 1 };
enum { BadKeyRest = 0xdeadbeef };
enum { CoresPerChip = MaxCores / MaxNumaNode };

#if !__APPLE__ && !defined(NOSUPERPAGE) && HAVE_SUPERPAGE_ENABLED
#define SUPERPAGE 1
#endif

//#define MEMSTATS 1
//#define GCSTATS 1
#define GETSTATS 0

#if GETSTATS
#define COMPSTATS 0     // measure the cycles spend in computation only
#define PMC_ENABLED  0  // enable pmc only if you have programmed pmc
#endif

#endif
