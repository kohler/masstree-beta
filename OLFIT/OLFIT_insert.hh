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
#ifndef OLFIT_INSERT_HH
#define OLFIT_INSERT_HH
#include "OLFIT_get.hh"
// #include "OLFIT_split.hh"
namespace OLFIT {

template <typename P>
bool tcursor<P>::find_insert(threadinfo& ti)
{
    find_locked(ti);
    original_n_ = n_;
    original_v_ = n_->full_unlocked_version_value();

    // maybe we found it
    if (state_)
        return true;

    // otherwise mark as inserted but not present
    state_ = 2;

    // mark insertion if we are changing modification state
    if (unlikely(n_->modstate_ != leaf<P>::modstate_insert)) {
        masstree_invariant(n_->modstate_ == leaf<P>::modstate_remove);
        n_->mark_insert();
        n_->modstate_ = leaf<P>::modstate_insert;
    }

    // try inserting into this node
    if (n_->size() < n_->width) {
        kx_.p = permuter_type(n_->permutation_).back();
        // don't inappropriately reuse position 0, which holds the ikey_bound
        if (likely(kx_.p != 0) || !n_->prev_ || n_->ikey_bound() == k_.ikey()) {
            n_->assign(kx_.p, k_, ti);
            return false;
        }
    }

    // otherwise must split
    return make_split(ti);
}

template <typename P>
void tcursor<P>::finish_insert()
{
    permuter_type perm(n_->permutation_);
    masstree_invariant(perm.back() == kx_.p);
    perm.insert_from_back(kx_.i);
    fence();
    n_->permutation_ = perm.value();
}

template <typename P>
inline void tcursor<P>::finish(int state, threadinfo& ti)
{
    if (state < 0 && state_ == 1) {
        if (finish_remove(ti))
            return;
    } else if (state > 0 && state_ == 2)
        finish_insert();
    // we finally know this!
    if (n_ == original_n_)
        updated_v_ = n_->full_unlocked_version_value();
    else
        new_nodes_.emplace_back(n_, n_->full_unlocked_version_value());
    n_->unlock();
}

} // namespace Masstree
#endif
