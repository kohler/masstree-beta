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
#include "kvio.hh"
#include "kvthread.hh"
#include <stdio.h>
#include <sys/mman.h>
#include <algorithm>
#include "clp.h"
#include <ctype.h>

static void *
initialize_page(void *page, const size_t pagesize, const size_t unit)
{
    void *first = (void *)iceil((uintptr_t)page, (uintptr_t)unit);
    assert((uintptr_t)first + unit <= (uintptr_t)page + pagesize);
    void **p = (void **)first;
    void *next = (void *)((uintptr_t)p + unit);
    while ((uintptr_t)next + unit <= (uintptr_t)page + pagesize) {
        *p = next;
        p = (void **)next;
        next = (void *)((uintptr_t)next + unit);
    }
    *p = NULL;
    return first;
}

namespace NormalPage {
struct allocator {
    static void *get_page(size_t nl) {
        assert(nl > 0);
        static const int PageSize = getpagesize();
        void *x = malloc(PageSize);
        assert(x);
        return initialize_page(x, PageSize, nl * CacheLineSize);
    }
};
}

#if __APPLE__ && !HAVE_DECL_GETLINE
static int
getline(char **, size_t *, FILE *)
{
  return 0; // XXX
}
#endif

size_t get_hugepage_size() {
    FILE *f = fopen("/proc/meminfo", "r");
    assert(f);
    char *linep = NULL;
    size_t n = 0;
    static const char *key = "Hugepagesize:";
    static const int keylen = strlen(key);
    size_t size = 0;
    while (getline(&linep, &n, f) > 0) {
        if (strstr(linep, key) != linep)
            continue;
        size = atol(linep + keylen) * 1024;
        break;
    }
    fclose(f);
    assert(size);
    return size;
}

namespace SuperPage {
struct allocator {
    static void *get_page(size_t nl) {
        static const int HugePageSize = get_hugepage_size();
        assert(nl > 0);
#ifdef MADV_HUGEPAGE
        void *x;
        int r = posix_memalign(&x, HugePageSize, HugePageSize);
        assert(!r);
        r = madvise(x, HugePageSize, MADV_HUGEPAGE);
        if (r) {
            perror("madvise");
            exit(EXIT_FAILURE);
        }
        return initialize_page(x, HugePageSize, nl * CacheLineSize);
#elif defined(MAP_HUGETLB)
        void *x = mmap(0, HugePageSize, PROT_READ | PROT_WRITE,
  	   	       MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (x == MAP_FAILED) {
            perror("mmap. Checking /proc/sys/vm/nr_hugepages that have preserved "
  	           "enough super-pages");
            assert(0);
        }
        return initialize_page(x, HugePageSize, nl * CacheLineSize);
#else
	(void) HugePageSize, (void) nl;
	mandatory_assert(0 && "No hugepage support is detected");
#endif
    }
};
}

#if SUPERPAGE
using namespace SuperPage;
#else
using namespace NormalPage;
#endif

void threadinfo::refill_aligned_arena(int nl)
{
    assert(!arena[nl - 1]);
    arena[nl - 1] = allocator::get_page(nl);
}

int clp_parse_suffixdouble(Clp_Parser *clp, const char *vstr,
			   int complain, void *)
{
    const char *post;
    if (*vstr == 0 || isspace((unsigned char) *vstr))
	post = vstr;
    else
	clp->val.d = strtod(vstr, (char **) &post);
    if (vstr != post && (*post == 'K' || *post == 'k'))
	clp->val.d *= 1000, ++post;
    else if (vstr != post && (*post == 'M' || *post == 'm'))
	clp->val.d *= 1000000, ++post;
    else if (vstr != post && (*post == 'B' || *post == 'b' || *post == 'G' || *post == 'g'))
	clp->val.d *= 1000000000, ++post;
    if (*vstr != 0 && *post == 0)
	return 1;
    else if (complain)
	return Clp_OptionError(clp, "%<%O%> expects a real number, not %<%s%>", vstr);
    else
	return 0;
}
