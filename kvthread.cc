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
#include "kvthread.hh"

threadinfo *threadinfo::allthreads;
pthread_key_t threadinfo::key;

#if HAVE_MEMDEBUG
void
memdebug::hard_free_checks(const memdebug *m, size_t size, int freetype,
			   int line, int after_rcu, const char *op) {
    if (m->magic == magic_free_value)
	fprintf(stderr, "%s(%p) @%d: double free, was @%d\n",
		op, m + 1, line, m->line);
    else if (m->magic != magic_value)
	fprintf(stderr, "%s(%p) @%d: freeing unallocated pointer (%x)\n",
		op, m + 1, line, m->magic);
    assert(m->magic == magic_value);
    if (freetype && m->freetype != freetype)
	fprintf(stderr, "%s(%p) @%d: expected type %x, saw %x, "
		"allocated %d\n", op, m + 1, line,
		freetype, m->freetype, m->line);
    if (!after_rcu && m->size != size)
	fprintf(stderr, "%s(%p) @%d: expected size %lu, saw %lu, "
		"allocated %d\n", op, m + 1, line,
		(unsigned long) size, (unsigned long) m->size, m->line);
    if (m->after_rcu != after_rcu)
	fprintf(stderr, "%s(%p) @%d: double free, rcu marked @%d\n",
		op, m + 1, line, m->line);
    if (freetype)
	assert(m->freetype == freetype);
    if (!after_rcu)
	assert(m->size == size);
    assert(m->after_rcu == after_rcu);
}

void
memdebug::hard_assert_use(const void *ptr, memtag tag1, memtag tag2) {
    const memdebug *m = reinterpret_cast<const memdebug *>(ptr) - 1;
    char buf[40];
    if (tag2 == (memtag) -1)
	sprintf(buf, "%x", tag1);
    else
	sprintf(buf, "%x/%x", tag1, tag2);
    if (m->magic == magic_free_value)
	fprintf(stderr, "%p: use tag %s after free\n",
		m + 1, buf);
    else if (m->magic != magic_value)
	fprintf(stderr, "%p: pointer is unallocated, not tag %s\n",
		m + 1, buf);
    assert(m->magic == magic_value);
    if (tag1 != 0 && (m->freetype >> 8) != tag1 && (m->freetype >> 8) != tag2)
	fprintf(stderr, "%p: expected tag %s, got tag %x\n",
		m + 1, buf, m->freetype >> 8);
    if (tag1 != 0)
	assert((m->freetype >> 8) == tag1 || (m->freetype >> 8) == tag2);
}
#endif

threadinfo *threadinfo::make(int purpose, int index)
{
    threadinfo *ti = (threadinfo *) malloc(8192);

    memset(ti, 0, sizeof(*ti));
    ti->ti_next = allthreads;
    ti->ti_purpose = purpose;
    ti->ti_index = index;
    ti->allthreads = ti;
    ti->pstat.initialize(index);
    ti->ts_ = 2;
    void *limbo_space = ti->allocate(sizeof(limbo_group), memtag_limbo, ta_rcu);
    ti->limbo_head_ = ti->limbo_tail_ = new(limbo_space) limbo_group;
    ti->pstat.mark_gc_alloc(ti->limbo_tail_->capacity);

    return ti;
}

void threadinfo::refill_rcu()
{
    if (limbo_head_ == limbo_tail_ && !limbo_tail_->next_
	&& limbo_tail_->head_ == limbo_tail_->tail_)
	limbo_tail_->head_ = limbo_tail_->tail_ = 0;
    else if (!limbo_tail_->next_) {
	void *limbo_space = allocate(sizeof(limbo_group), memtag_limbo, ta_rcu);
	limbo_tail_->next_ = new(limbo_space) limbo_group;
	limbo_tail_ = limbo_tail_->next_;
	pstat.mark_gc_alloc(limbo_tail_->capacity);
    } else
	limbo_tail_ = limbo_tail_->next_;
}

void threadinfo::hard_rcu_quiesce()
{
    uint64_t min_epoch = gc_epoch;
    for (threadinfo *ti = allthreads; ti; ti = ti->ti_next) {
	prefetch((const void *) ti->ti_next);
	uint64_t ti_epoch = ti->gc_epoch;
	if (ti_epoch && (int64_t) (ti_epoch - min_epoch) < 0)
	    min_epoch = ti_epoch;
    }

    limbo_group *lg = limbo_head_;
    limbo_element *lb = &lg->e_[lg->head_];
    limbo_element *le = &lg->e_[lg->tail_];

    if (lb != le && (int64_t) (lb->epoch_ - min_epoch) < 0) {
	while (1) {
	    free_rcu(lb->ptr_, lb->freetype_);
	    pstat.mark_gc_object_freed();

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
			ti_index, lg, i, status ? "waiting" : "freed",
			lg->e_[i].freetype_, lg->e_[i].epoch_);
	}
    }
}

void threadinfo::report_rcu_all(void *ptr)
{
    for (threadinfo *ti = allthreads; ti; ti = ti->ti_next)
	ti->report_rcu(ptr);
}
