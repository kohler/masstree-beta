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
#ifndef KVTHREAD_HH
#define KVTHREAD_HH 1
#include "mtcounters.hh"
#include "compiler.hh"
#include "circular_int.hh"
#include "timestamp.hh"
#include <assert.h>
#include <pthread.h>
#include <sys/mman.h>
#include <stdlib.h>

class threadinfo;
class loginfo;

extern volatile uint64_t globalepoch;    // global epoch, updated regularly
extern volatile bool recovering;

struct memdebug {
#if HAVE_MEMDEBUG
    enum {
	magic_value = 389612313 /* = 0x17390319 */,
	magic_free_value = 2015593488 /* = 0x78238410 */
    };
    int magic;
    int freetype;
    size_t size;
    int after_rcu;
    int line;

    static void *make(void *p, size_t size, int freetype, int line = 0) {
	if (p) {
	    memdebug *m = reinterpret_cast<memdebug *>(p);
	    m->magic = magic_value;
	    m->freetype = freetype;
	    m->size = size;
	    m->after_rcu = 0;
	    m->line = line;
	    return m + 1;
	} else
	    return p;
    }
    static void *check_free(void *p, size_t size, int freetype, int line = 0) {
	memdebug *m = reinterpret_cast<memdebug *>(p) - 1;
	free_checks(m, size, freetype, line, false, "deallocate");
	m->magic = magic_free_value;
	return m;
    }
    static void check_rcu(void *p, size_t size, int freetype, int line = 0) {
	memdebug *m = reinterpret_cast<memdebug *>(p) - 1;
	free_checks(m, size, freetype, line, false, "deallocate_rcu");
	m->after_rcu = 1;
	m->line = line;
    }
    static void *check_free_after_rcu(void *p, int freetype) {
	memdebug *m = reinterpret_cast<memdebug *>(p) - 1;
	free_checks(m, 0, freetype, 0, true, "free_after_rcu");
	m->magic = magic_free_value;
	return m;
    }
    static bool check_use(const void *p, int type) {
	const memdebug *m = reinterpret_cast<const memdebug *>(p) - 1;
	return m->magic == magic_value && (type == 0 || (m->freetype >> 8) == type);
    }
    static bool check_use(const void *p, int type1, int type2) {
	const memdebug *m = reinterpret_cast<const memdebug *>(p) - 1;
	return m->magic == magic_value
	    && ((m->freetype >> 8) == type1 || (m->freetype >> 8) == type2);
    }
    static void assert_use(const void *p, memtag tag) {
	if (!check_use(p, tag))
	    hard_assert_use(p, tag, (memtag) -1);
    }
    static void assert_use(const void *p, memtag tag1, memtag tag2) {
	if (!check_use(p, tag1, tag2))
	    hard_assert_use(p, tag1, tag2);
    }
  private:
    static void free_checks(const memdebug *m, size_t size, int freetype,
			    int line, int after_rcu, const char *op) {
	if (m->magic != magic_value
	    || m->freetype != freetype
	    || (!after_rcu && m->size != size)
	    || m->after_rcu != after_rcu)
	    hard_free_checks(m, freetype, size, line, after_rcu, op);
    }
    static void hard_free_checks(const memdebug *m, size_t size, int freetype,
				 int line, int after_rcu, const char *op);
    static void hard_assert_use(const void *ptr, memtag tag1, memtag tag2);
#else
    static void *make(void *p, size_t, int, int = 0) {
	return p;
    }
    static void *check_free(void *p, size_t, int, int = 0) {
	return p;
    }
    static void check_rcu(void *, size_t, int, int = 0) {
    }
    static void *check_free_after_rcu(void *p, int) {
	return p;
    }
    static bool check_use(void *, memtag) {
	return true;
    }
    static bool check_use(void *, memtag, memtag) {
	return true;
    }
    static void assert_use(void *, memtag) {
    }
    static void assert_use(void *, memtag, memtag) {
    }
#endif
};

enum {
#if HAVE_MEMDEBUG
    memdebug_size = sizeof(memdebug)
#else
    memdebug_size = 0
#endif
};

struct limbo_element {
    void *ptr_;
    int freetype_;
    uint64_t epoch_;
};

struct limbo_group {
    enum { capacity = (4076 - sizeof(limbo_group *)) / sizeof(limbo_element) };
    int head_;
    int tail_;
    limbo_element e_[capacity];
    limbo_group *next_;
    limbo_group()
	: head_(0), tail_(0), next_() {
    }
    void push_back(void *ptr, int freetype, uint64_t epoch) {
	assert(tail_ < capacity);
	e_[tail_].ptr_ = ptr;
	e_[tail_].freetype_ = freetype;
	e_[tail_].epoch_ = epoch;
	++tail_;
    }
};

template <int N> struct has_threadcounter {
    static bool test(threadcounter ci) {
	return unsigned(ci) < unsigned(N);
    }
};
template <> struct has_threadcounter<0> {
    static bool test(threadcounter) {
	return false;
    }
};

struct rcu_callback {
    virtual ~rcu_callback() {
    }
    virtual void operator()(threadinfo& ti) = 0;
};

class threadinfo {
  public:

    enum {
	TI_MAIN, TI_PROCESS, TI_LOG, TI_CHECKPOINT
    };

    union {
	struct {
	    threadinfo *ti_next;
	    pthread_t ti_threadid;
	    int ti_purpose;
	    int ti_index;	    // the index of a udp, logging, tcp,
				    // checkpoint or recover thread
	    int ti_pipe[2];         // the pipe used to communicate with the thread
	    loginfo *ti_log;
	    uint64_t gc_epoch;
	    uint64_t limbo_epoch_;
	};
	char padding1[CACHE_LINE_SIZE];
    };

  private:
    enum { NMaxLines = 20 };
    void *arena[NMaxLines];

  public:
    char buf_[64];
    int64_t n_delta_markers_;

  private:
    limbo_group *limbo_head_;
    limbo_group *limbo_tail_;
    mutable kvtimestamp_t ts_;

  public:
    static threadinfo *make(int purpose, int index);
    // XXX destructor
    static threadinfo *allthreads;
    static pthread_key_t key;

    // timestamps
    kvtimestamp_t operation_timestamp() const {
	return timestamp();
    }
    kvtimestamp_t update_timestamp() const {
	return ts_;
    }
    kvtimestamp_t update_timestamp(kvtimestamp_t x) const {
	if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
	    // x might be a marker timestamp; ensure result is not
	    ts_ = (x | 1) + 1;
	return ts_;
    }
    kvtimestamp_t update_timestamp(kvtimestamp_t x, kvtimestamp_t y) const {
	if (circular_int<kvtimestamp_t>::less(x, y))
	    x = y;
	if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
	    // x might be a marker timestamp; ensure result is not
	    ts_ = (x | 1) + 1;
	return ts_;
    }
    void increment_timestamp() {
	ts_ += 2;
    }
    void advance_timestamp(kvtimestamp_t x) {
	if (circular_int<kvtimestamp_t>::less(ts_, x))
	    ts_ = x;
    }

    // event counters
    void mark(threadcounter ci) {
	if (has_threadcounter<int(ncounters)>::test(ci))
	    ++counters_[ci];
    }
    void mark(threadcounter ci, int64_t delta) {
        if (has_threadcounter<int(ncounters)>::test(ci))
            counters_[ci] += delta;
    }
    bool has_counter(threadcounter ci) const {
        return has_threadcounter<int(ncounters)>::test(ci);
    }
    uint64_t counter(threadcounter ci) const {
	return has_threadcounter<int(ncounters)>::test(ci) ? counters_[ci] : 0;
    }

    struct accounting_relax_fence_function {
	threadinfo *ti_;
	threadcounter ci_;
	accounting_relax_fence_function(threadinfo *ti, threadcounter ci)
	    : ti_(ti), ci_(ci) {
	}
	void operator()() {
	    relax_fence();
	    ti_->mark(ci_);
	}
    };
    /** @brief Return a function object that calls mark(ci); relax_fence().
     *
     * This function object can be used to count the number of relax_fence()s
     * executed. */
    accounting_relax_fence_function accounting_relax_fence(threadcounter ci) {
	return accounting_relax_fence_function(this, ci);
    }

    struct stable_accounting_relax_fence_function {
	threadinfo *ti_;
	stable_accounting_relax_fence_function(threadinfo *ti)
	    : ti_(ti) {
	}
	template <typename V>
	void operator()(V v) {
	    relax_fence();
	    ti_->mark(threadcounter(tc_stable + (v.isleaf() << 1) + v.splitting()));
	}
    };
    /** @brief Return a function object that calls mark(ci); relax_fence().
     *
     * This function object can be used to count the number of relax_fence()s
     * executed. */
    stable_accounting_relax_fence_function stable_fence() {
	return stable_accounting_relax_fence_function(this);
    }

    accounting_relax_fence_function lock_fence(threadcounter ci) {
	return accounting_relax_fence_function(this, ci);
    }

    // memory allocation
    void* allocate(size_t sz, memtag tag, int line = 0) {
	void *p = malloc(sz + memdebug_size);
	p = memdebug::make(p, sz, tag << 8, line);
	if (p)
            mark(threadcounter(tc_alloc + (tag > memtag_value)), sz);
	return p;
    }
    void deallocate(void* p, size_t sz, memtag tag, int line = 0) {
	// in C++ allocators, 'p' must be nonnull
	assert(p);
	p = memdebug::check_free(p, sz, tag << 8, line);
	free(p);
        mark(threadcounter(tc_alloc + (tag > memtag_value)), -sz);
    }
    void deallocate_rcu(void *p, size_t sz, memtag tag, int line = 0) {
	assert(p);
	memdebug::check_rcu(p, sz, tag << 8, line);
	record_rcu(p, tag << 8);
        mark(threadcounter(tc_alloc + (tag > memtag_value)), -sz);
    }

    static size_t aligned_size(size_t sz) {
	return iceil(sz, int(CACHE_LINE_SIZE));
    }
    void* allocate_aligned(size_t sz, memtag tag, int line = 0) {
	int nl = (sz + memdebug_size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
	assert(nl < NMaxLines);
	if (unlikely(!arena[nl - 1]))
	    refill_aligned_arena(nl);
	void *p = arena[nl - 1];
	if (p)
	    arena[nl - 1] = *reinterpret_cast<void **>(p);
	p = memdebug::make(p, sz, (tag << 8) + nl, line);
	if (p)
            mark(threadcounter(tc_alloc + (tag > memtag_value)),
                 nl * CACHE_LINE_SIZE);
	return p;
    }
    void deallocate_aligned(void* p, size_t sz, memtag tag, int line = 0) {
	assert(p);
	int nl = (sz + memdebug_size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
	p = memdebug::check_free(p, sz, (tag << 8) + nl, line);
	*reinterpret_cast<void **>(p) = arena[nl - 1];
	arena[nl - 1] = p;
        mark(threadcounter(tc_alloc + (tag > memtag_value)),
             -nl * CACHE_LINE_SIZE);
    }
    void deallocate_aligned_rcu(void* p, size_t sz, memtag tag, int line = 0) {
	assert(p);
	int nl = (sz + memdebug_size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
	memdebug::check_rcu(p, sz, (tag << 8) + nl, line);
	record_rcu(p, (tag << 8) + nl);
        mark(threadcounter(tc_alloc + (tag > memtag_value)),
             -nl * CACHE_LINE_SIZE);
    }

    // RCU
    void rcu_start() {
	if (gc_epoch != globalepoch)
	    gc_epoch = globalepoch;
    }
    void rcu_stop() {
	if (limbo_epoch_ && (gc_epoch - limbo_epoch_) > 1)
	    hard_rcu_quiesce();
	gc_epoch = 0;
    }
    void rcu_quiesce() {
	rcu_start();
	if (limbo_epoch_ && (gc_epoch - limbo_epoch_) > 2)
	    hard_rcu_quiesce();
    }
    typedef ::rcu_callback rcu_callback;
    void rcu_register(rcu_callback* cb) {
	record_rcu(cb, -1);
    }

    void enter() {
	ti_threadid = pthread_self();
	pthread_setspecific(key, this);
    }
    static threadinfo *current() {
	return (threadinfo *) pthread_getspecific(key);
    }

    void report_rcu(void *ptr) const;
    static void report_rcu_all(void *ptr);

  private:

    //enum { ncounters = (int) tc_max };
    enum { ncounters = 0 };
    uint64_t counters_[ncounters];

    void refill_aligned_arena(int nl);
    void refill_rcu();

    void free_rcu(void *p, int freetype) {
	if ((freetype & 255) == 0) {
	    p = memdebug::check_free_after_rcu(p, freetype);
	    ::free(p);
	} else if (freetype == -1)
	    (*static_cast<rcu_callback *>(p))(*this);
	else {
	    p = memdebug::check_free_after_rcu(p, freetype);
	    int nl = freetype & 255;
	    *reinterpret_cast<void **>(p) = arena[nl - 1];
	    arena[nl - 1] = p;
	}
    }

    void record_rcu(void* ptr, int freetype) {
	if (recovering && freetype == (memtag_value << 8)) {
	    free_rcu(ptr, freetype);
	    return;
	}
	if (limbo_tail_->tail_ == limbo_tail_->capacity)
	    refill_rcu();
	uint64_t epoch = globalepoch;
	limbo_tail_->push_back(ptr, freetype, epoch);
	if (!limbo_epoch_)
	    limbo_epoch_ = epoch;
    }

    void hard_rcu_quiesce();
    friend class loginfo;

};

#endif
