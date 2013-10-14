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
#ifndef MASSTREE_INSERT_HH
#define MASSTREE_INSERT_HH
#include "masstree_get.hh"
#include "masstree_split.hh"
namespace Masstree {

template <typename P>
inline node_base<P>* tcursor<P>::check_leaf_insert(node_type* root,
                                                   nodeversion_type v,
                                                   threadinfo& ti)
{
    if (node_type* next_root = get_leaf_locked(root, v, ti))
	return next_root;

    if (kp_ >= 0) {
	if (n_->ksuf_equals(kp_, ka_))
	    return found_marker();
	// Must create new layer
	key_type oka(n_->ksuf(kp_));
	ka_.shift();
	int kc = key_compare(oka, ka_);
	// Estimate how much space will be required for keysuffixes
	size_t ksufsize;
	if (kc && (ka_.has_suffix() || oka.has_suffix()))
	    ksufsize = (std::max(0, ka_.suffix_length())
			+ std::max(0, oka.suffix_length())) * (n_->width / 2)
		+ n_->iksuf_[0].overhead(n_->width);
	else
	    ksufsize = 0;
	leaf_type *nl = leaf_type::make_root(ksufsize, n_, ti);
	nl->assign_initialize(0, kc <= 0 ? oka : ka_, ti);
	if (kc != 0)
	    nl->assign_initialize(1, kc <= 0 ? ka_ : oka, ti);
	nl->lv_[kc > 0] = n_->lv_[kp_];
	if (kc != 0) {
	    nl->lock(*nl, ti.lock_fence(tc_leaf_lock));
	    nl->lv_[kc < 0] = leafvalue_type::make_empty();
	}
	if (kc <= 0)
	    nl->permutation_ = permuter_type::make_sorted(1);
	else {
	    permuter_type permnl = permuter_type::make_sorted(2);
	    permnl.remove_to_back(0);
	    nl->permutation_ = permnl.value();
	}
	// In a prior version, recursive tree levels and true values were
	// differentiated by a bit in the leafvalue. But this constrains the
	// values users could assign for true values. So now we use bits in
	// the key length, and changing a leafvalue from true value to
	// recursive tree requires two writes. How to make this work in the
	// face of concurrent lockless readers? We do it with two bits and
	// retry. The first keylenx_ write informs a reader that the value is
	// in flux, the second informs it of the true value. On x86 we only
	// need compiler barriers.
	n_->keylenx_[kp_] = sizeof(n_->ikey0_[0]) + 65;
	fence();
	n_->lv_[kp_] = nl;
	fence();
	n_->keylenx_[kp_] = sizeof(n_->ikey0_[0]) + 129;
	n_->unlock(v);
	if (kc != 0) {
	    n_ = nl;
	    ki_ = kp_ = kc < 0;
	    return insert_marker();
	} else
	    return nl;
    }

    // insert
 do_insert:
    if (n_->size() + n_->nremoved_ < n_->width) {
	kp_ = permuter_type(n_->permutation_).back();
	// watch out for attempting to use position 0
	if (likely(kp_ != 0) || !n_->prev_ || n_->ikey_bound() == ka_.ikey()) {
	    n_->assign(kp_, ka_, ti);
	    return insert_marker();
	}
    }

    // if there have been removals, reuse their space
    if (n_->nremoved_ > 0) {
	if (unlikely(n_->deleted_layer())) {
	    n_->unlock(v);
	    return reset_retry();
	}
	// since keysuffixes might change as we reassign keys,
	// mark change so observers retry
	n_->mark_insert(v);
	n_->nremoved_ = 0;
	// Position 0 is hard to reuse; ensure we reuse it last
	permuter_type perm(n_->permutation_);
	int zeroidx = find_lowest_zero_nibble(perm.value_from(0));
	masstree_invariant(perm[zeroidx] == 0 && zeroidx < n_->width);
	if (zeroidx > perm.size() && n_->prev_) {
	    perm.exchange(perm.size(), zeroidx);
	    n_->permutation_ = perm.value();
	    fence();
	}
	goto do_insert;
    }

    // split
    return finish_split(ti);
}

template <typename P>
bool tcursor<P>::find_insert(threadinfo& ti)
{
    node_type* root = root_;
    nodeversion_type v;
    while (1) {
	n_ = root->reach_leaf(ka_, v, ti);
	root = check_leaf_insert(root, v, ti);
	if (reinterpret_cast<uintptr_t>(root) <= reinterpret_cast<uintptr_t>(insert_marker())) {
            state_ = 2 + (root == found_marker());
	    return root == found_marker();
        }
    }
}

template <typename P>
void tcursor<P>::finish_insert()
{
    permuter_type perm(n_->permutation_);
    masstree_invariant(perm.back() == kp_);
    perm.insert_from_back(ki_);
    fence();
    n_->permutation_ = perm.value();
}

template <typename P>
inline void tcursor<P>::finish(int state, threadinfo& ti)
{
    if (state < 0 && (state_ & 1)) {
        if (finish_remove(ti))
            return;
    } else if (state > 0 && state_ == 2)
        finish_insert();
    n_->unlock();
}

template <typename P> template <typename F>
inline int basic_table<P>::modify(Str key, F& f, threadinfo& ti)
{
    tcursor<P> lp(*this, key);
    bool found = lp.find_locked(ti);
    int answer;
    if (found)
	answer = f(key, true, lp.value(), ti, lp.node_timestamp());
    else
	answer = 0;
    lp.finish(answer, ti);
    return answer;
}

template <typename P> template <typename F>
inline int basic_table<P>::modify_insert(Str key, F& f, threadinfo& ti)
{
    tcursor<P> lp(*this, key);
    bool found = lp.find_insert(ti);
    if (!found)
	ti.advance_timestamp(lp.node_timestamp());
    int answer = f(key, found, lp.value(), ti, lp.node_timestamp());
    lp.finish(answer, ti);
    return answer;
}

} // namespace Masstree
#endif
