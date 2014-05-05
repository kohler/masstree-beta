/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2014 President and Fellows of Harvard College
 * Copyright (c) 2012-2014 Massachusetts Institute of Technology
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
pthread_key_t threadinfo::key;
#if ENABLE_ASSERTIONS
int threadinfo::no_pool_value;
#endif

#if HAVE_MEMDEBUG
void memdebug::landmark(char* buf, size_t size) const {
    if (this->magic != magic_value && this->magic != magic_free_value)
        snprintf(buf, size, "???");
    else if (this->file)
        snprintf(buf, size, "%s:%d", this->file, this->line);
    else if (this->line)
        snprintf(buf, size, "%d", this->line);
    else
        snprintf(buf, size, "0");
}

void
memdebug::hard_free_checks(const memdebug *m, size_t size, int freetype,
                           int after_rcu, const char *op) {
    char buf[256];
    m->landmark(buf, sizeof(buf));
    if (m->magic == magic_free_value)
        fprintf(stderr, "%s(%p): double free, was @%s\n",
                op, m + 1, buf);
    else if (m->magic != magic_value)
        fprintf(stderr, "%s(%p): freeing unallocated pointer (%x)\n",
                op, m + 1, m->magic);
    assert(m->magic == magic_value);
    if (freetype && m->freetype != freetype)
        fprintf(stderr, "%s(%p): expected type %x, saw %x, "
                "allocated %s\n", op, m + 1, freetype, m->freetype, buf);
    if (!after_rcu && m->size != size)
        fprintf(stderr, "%s(%p): expected size %lu, saw %lu, "
                "allocated %s\n", op, m + 1,
                (unsigned long) size, (unsigned long) m->size, buf);
    if (m->after_rcu != after_rcu)
        fprintf(stderr, "%s(%p): double free after rcu, allocated @%s\n",
                op, m + 1, buf);
    if (freetype)
        assert(m->freetype == freetype);
    if (!after_rcu)
        assert(m->size == size);
    assert(m->after_rcu == after_rcu);
}

void
memdebug::hard_assert_use(const void *ptr, memtag tag1, memtag tag2) {
    const memdebug *m = reinterpret_cast<const memdebug *>(ptr) - 1;
    char tagbuf[40], buf[256];
    m->landmark(buf, sizeof(buf));
    if (tag2 == (memtag) -1)
        sprintf(buf, "%x", tag1);
    else
        sprintf(buf, "%x/%x", tag1, tag2);
    if (m->magic == magic_free_value)
        fprintf(stderr, "%p: use tag %s after free, allocated %s\n",
                m + 1, tagbuf, buf);
    else if (m->magic != magic_value)
        fprintf(stderr, "%p: pointer is unallocated, not tag %s\n",
                m + 1, tagbuf);
    assert(m->magic == magic_value);
    if (tag1 != 0 && (m->freetype >> 8) != tag1 && (m->freetype >> 8) != tag2)
        fprintf(stderr, "%p: expected tag %s, got tag %x, allocated %s\n",
                m + 1, tagbuf, m->freetype >> 8, buf);
    if (tag1 != 0)
        assert((m->freetype >> 8) == tag1 || (m->freetype >> 8) == tag2);
}
#endif

threadinfo *threadinfo::make(int purpose, int index) {
    static int threads_initialized;

    threadinfo *ti = (threadinfo *) malloc(8192);
    memset(ti, 0, sizeof(*ti));
    ti->next_ = allthreads;
    ti->purpose_ = purpose;
    ti->index_ = index;
    ti->allthreads = ti;
    ti->ts_ = 2;
    void *limbo_space = ti->allocate(sizeof(limbo_group), memtag_limbo);
    ti->mark(tc_limbo_slots, limbo_group::capacity);
    ti->limbo_head_ = ti->limbo_tail_ = new(limbo_space) limbo_group;

    if (!threads_initialized) {
#if ENABLE_ASSERTIONS
        const char* s = getenv("_");
        no_pool_value = s && strstr(s, "valgrind") != 0;
#endif
        threads_initialized = 1;
    }

    return ti;
}

void threadinfo::refill_rcu()
{
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

void threadinfo::hard_rcu_quiesce()
{
    uint64_t min_epoch = gc_epoch_;
    for (threadinfo *ti = allthreads; ti; ti = ti->next()) {
        prefetch((const void *) ti->next());
        uint64_t epoch = ti->gc_epoch_;
        if (epoch && (int64_t) (epoch - min_epoch) < 0)
            min_epoch = epoch;
    }

    limbo_group *lg = limbo_head_;
    limbo_element *lb = &lg->e_[lg->head_];
    limbo_element *le = &lg->e_[lg->tail_];

    if (lb != le && (int64_t) (lb->epoch_ - min_epoch) < 0) {
        while (1) {
            free_rcu(lb->ptr_, lb->freetype_);
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
                        lg->e_[i].freetype_, lg->e_[i].epoch_);
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

void threadinfo::run() {
    threadid_ = pthread_self();
    pthread_setspecific(key, this);
}

void* threadinfo::thread_trampoline(void* argument) {
    threadinfo* ti = static_cast<threadinfo*>(argument);
    ti->run();
    return ti->thread_func_(ti);
}

int threadinfo::run(void* (*thread_func)(threadinfo*), void* thread_data) {
    assert(!thread_func_ && !threadid_);
    thread_func_ = thread_func;
    thread_data_ = thread_data;
    return pthread_create(&threadid_, 0, thread_trampoline, this);
}
