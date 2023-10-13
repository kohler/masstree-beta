
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
#ifndef OLFIT_GET_HH
#define OLFIT_GET_HH
#include "OLFIT_tcursor.hh"
#include "../fixsizedkey.hh"
namespace OLFIT {

template <typename P>
bool unlocked_tcursor<P>::find_unlocked(threadinfo& ti)
{
    int match;
    key_indexed_position kx;
    node_base<P>* root = const_cast<node_base<P>*>(root_);

 retry:
    n_ = root->reach_leaf(k_, v_, ti);

 forward:
    if (v_.deleted())
        goto retry;

    n_->prefetch();
    perm_ = n_->permutation();
    kx = leaf<P>::bound_type::lower(k_, *this);
    if (kx.p >= 0) {
        value_ = n_->v_[kx.p];
        match = 1;
    } else
        match = 0;
    if (n_->has_changed(v_)) {
        ti.mark(threadcounter(tc_stable_leaf_insert + n_->simple_has_split(v_)));
        n_ = n_->advance_to_key(k_, v_, ti);
        goto forward;
    }

    return match;
}

template <typename P>
inline bool basic_table<P>::get(Str key, value_type &value,
                                threadinfo& ti) const
{
    unlocked_tcursor<P> lp(*this, key);
    bool found = lp.find_unlocked(ti);
    if (found)
        value = lp.value();
    return found;
}

template <typename P>
bool tcursor<P>::find_locked(threadinfo& ti)
{
    node_base<P>* root = const_cast<node_base<P>*>(root_);
    nodeversion_type v;
    permuter_type perm;

 retry:
    n_ = root->reach_leaf(k_, v, ti);

 forward:
    if (v.deleted())
        goto retry;

    n_->prefetch();
    perm = n_->permutation();
    fence();
    kx_ = leaf<P>::bound_type::lower(k_, *n_);
    if (kx_.p >= 0) {
        value_type v = n_->v_[kx_.p];
        state_ = 1;
    } else
        state_ = 0;

    n_->lock(v, ti.lock_fence(tc_leaf_lock));
    if (n_->has_changed(v) || n_->permutation() != perm) {
        ti.mark(threadcounter(tc_stable_leaf_insert + n_->simple_has_split(v)));
        n_->unlock();
        n_ = n_->advance_to_key(k_, v, ti);
        goto forward;
    }
    return state_;
}

} // namespace Masstree
#endif
