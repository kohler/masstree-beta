/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2016 President and Fellows of Harvard College
 * Copyright (c) 2012-2016 Massachusetts Institute of Technology
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
#include "kvthread.hh"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <new>
#include <sys/mman.h>
#if HAVE_SUPERPAGE && !NOSUPERPAGE
#include <sys/types.h>
#include <dirent.h>
#endif

threadinfo *threadinfo::allthreads;
#if ENABLE_ASSERTIONS
int threadinfo::no_pool_value;
#endif

inline threadinfo::threadinfo(int purpose, int index) {
    memset(this, 0, sizeof(*this));
    purpose_ = purpose;
    index_ = index;

    void *limbo_space = allocate(sizeof(limbo_group), memtag_limbo);
    mark(tc_limbo_slots, limbo_group::capacity);
    limbo_head_ = limbo_tail_ = new(limbo_space) limbo_group;
    ts_ = 2;
}

threadinfo *threadinfo::make(int purpose, int index) {
    static int threads_initialized;

    threadinfo* ti = new(malloc(8192)) threadinfo(purpose, index);
    ti->next_ = allthreads;
    allthreads = ti;

    if (!threads_initialized) {
#if ENABLE_ASSERTIONS
        const char* s = getenv("_");
        no_pool_value = s && strstr(s, "valgrind") != 0;
#endif
        threads_initialized = 1;
    }

    return ti;
}

void threadinfo::refill_rcu() {
    if (limbo_head_ == limbo_tail_ && !limbo_tail_->next_
        && limbo_tail_->head_ == limbo_tail_->tail_)
        limbo_tail_->head_ = limbo_tail_->tail_ = 0;
    else if (!limbo_tail_->next_) {
        void *limbo_space = allocate(sizeof(limbo_group), memtag_limbo);
        mark(tc_limbo_slots, limbo_group::capacity);
        limbo_tail_->next_ = new(limbo_space) limbo_group;
        limbo_tail_ = limbo_tail_->next_;
    } else
        limbo_tail_ = limbo_tail_->next_;
}

void threadinfo::hard_rcu_quiesce() {
    mrcu_epoch_type min_epoch = gc_epoch_;
    for (threadinfo *ti = allthreads; ti; ti = ti->next()) {
        prefetch((const void *) ti->next());
        mrcu_epoch_type epoch = ti->gc_epoch_;
        if (epoch && mrcu_signed_epoch_type(epoch - min_epoch) < 0)
            min_epoch = epoch;
    }

    limbo_group *lg = limbo_head_;
    limbo_element *lb = &lg->e_[lg->head_];
    limbo_element *le = &lg->e_[lg->tail_];

    if (lb != le && (int64_t) (lb->epoch_ - min_epoch) < 0) {
        while (1) {
            free_rcu(lb->ptr_, lb->tag_);
            mark(tc_gc);

            ++lb;

            if (lb == le && lg == limbo_tail_) {
                lg->head_ = lg->tail_;
                break;
            } else if (lb == le) {
                assert(lg->tail_ == lg->capacity && lg->next_);
                lg->head_ = lg->tail_ = 0;
                lg = lg->next_;
                lb = &lg->e_[lg->head_];
                le = &lg->e_[lg->tail_];
            } else if (lb->epoch_ < min_epoch) {
                lg->head_ = lb - lg->e_;
                break;
            }
        }

        if (lg != limbo_head_) {
            // shift nodes in [limbo_head_, limbo_tail_) to be after
            // limbo_tail_
            limbo_group *old_head = limbo_head_;
            limbo_head_ = lg;
            limbo_group **last = &limbo_tail_->next_;
            while (*last)
                last = &(*last)->next_;
            *last = old_head;
            while (*last != lg)
                last = &(*last)->next_;
            *last = 0;
        }
    }

    limbo_epoch_ = (lb == le ? 0 : lb->epoch_);
}

void threadinfo::report_rcu(void *ptr) const
{
    for (limbo_group *lg = limbo_head_; lg; lg = lg->next_) {
        int status = 0;
        for (int i = 0; i < lg->capacity; ++i) {
            if (i == lg->head_)
                status = 1;
            if (i == lg->tail_)
                status = 0;
            if (lg->e_[i].ptr_ == ptr)
                fprintf(stderr, "thread %d: rcu %p@%d: %s as %x @%" PRIu64 "\n",
                        index_, lg, i, status ? "waiting" : "freed",
                        lg->e_[i].tag_, lg->e_[i].epoch_);
        }
    }
}

void threadinfo::report_rcu_all(void *ptr)
{
    for (threadinfo *ti = allthreads; ti; ti = ti->next())
        ti->report_rcu(ptr);
}


#if HAVE_SUPERPAGE && !NOSUPERPAGE
static size_t read_superpage_size() {
    if (DIR* d = opendir("/sys/kernel/mm/hugepages")) {
        size_t n = (size_t) -1;
        while (struct dirent* de = readdir(d))
            if (de->d_type == DT_DIR
                && strncmp(de->d_name, "hugepages-", 10) == 0
                && de->d_name[10] >= '0' && de->d_name[10] <= '9') {
                size_t x = strtol(&de->d_name[10], 0, 10) << 10;
                n = (x < n ? x : n);
            }
        closedir(d);
        return n;
    } else
        return 2 << 20;
}

static size_t superpage_size = 0;
#endif

static void initialize_pool(void* pool, size_t sz, size_t unit) {
    char* p = reinterpret_cast<char*>(pool);
    void** nextptr = reinterpret_cast<void**>(p);
    for (size_t off = unit; off + unit <= sz; off += unit) {
        *nextptr = p + off;
        nextptr = reinterpret_cast<void**>(p + off);
    }
    *nextptr = 0;
}

void threadinfo::refill_pool(int nl) {
    assert(!pool_[nl - 1]);

    if (!use_pool()) {
        pool_[nl - 1] = malloc(nl * CACHE_LINE_SIZE);
        if (pool_[nl - 1])
            *reinterpret_cast<void**>(pool_[nl - 1]) = 0;
        return;
    }

    void* pool = 0;
    size_t pool_size = 0;
    int r;

#if HAVE_SUPERPAGE && !NOSUPERPAGE
    if (!superpage_size)
        superpage_size = read_superpage_size();
    if (superpage_size != (size_t) -1) {
        pool_size = superpage_size;
# if MADV_HUGEPAGE
        if ((r = posix_memalign(&pool, pool_size, pool_size)) != 0) {
            fprintf(stderr, "posix_memalign superpage: %s\n", strerror(r));
            pool = 0;
            superpage_size = (size_t) -1;
        } else if (madvise(pool, pool_size, MADV_HUGEPAGE) != 0) {
            perror("madvise superpage");
            superpage_size = (size_t) -1;
        }
# elif MAP_HUGETLB
        pool = mmap(0, pool_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (pool == MAP_FAILED) {
            perror("mmap superpage");
            pool = 0;
            superpage_size = (size_t) -1;
        }
# else
        superpage_size = (size_t) -1;
# endif
    }
#endif

    if (!pool) {
        pool_size = 2 << 20;
        if ((r = posix_memalign(&pool, CACHE_LINE_SIZE, pool_size)) != 0) {
            fprintf(stderr, "posix_memalign: %s\n", strerror(r));
            abort();
        }
    }

    initialize_pool(pool, pool_size, nl * CACHE_LINE_SIZE);
    pool_[nl - 1] = pool;
}
