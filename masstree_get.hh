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
#ifndef MASSTREE_GET_HH
#define MASSTREE_GET_HH 1
#include "masstree_tcursor.hh"
#include "masstree_key.hh"
namespace Masstree {

template <typename P>
bool unlocked_tcursor<P>::find_unlocked(threadinfo& ti)
{
    int match = 0;
    key_indexed_position kx;
    node_base<P>* root = const_cast<node_base<P>*>(root_);

 retry:
    n_ = root->reach_leaf(ka_, v_, ti);

 forward:
    if (v_.deleted())
        goto retry;

    n_->prefetch();
    perm_ = n_->permutation();
    kx = leaf<P>::bound_type::lower(ka_, *this);
    if (kx.p >= 0) {
        lv_ = n_->lv_[kx.p];
        lv_.prefetch(n_->keylenx_[kx.p]);
        match = n_->ksuf_matches(kx.p, ka_);
    }
    if (n_->has_changed(v_)) {
        ti.mark(threadcounter(tc_stable_leaf_insert + n_->simple_has_split(v_)));
        n_ = n_->advance_to_key(ka_, v_, ti);
        goto forward;
    }

    if (match < 0) {
        ka_.shift_by(-match);
        root = lv_.layer();
        goto retry;
    } else
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
inline node_base<P>* tcursor<P>::get_leaf_locked(node_type* root,
                                                 nodeversion_type& v,
                                                 threadinfo& ti)
{
    nodeversion_type oldv = v;
    typename permuter_type::storage_type old_perm;
    leaf_type *next;

    n_->prefetch();

    if (!ka_.has_suffix())
        v = n_->lock(oldv, ti.lock_fence(tc_leaf_lock));
    else {
        // First, look up without locking.
        // The goal is to avoid dirtying cache lines on upper layers of a long
        // key walk. But we do lock if the next layer has split.
        old_perm = n_->permutation_;
        kx_ = leaf_type::bound_type::lower(ka_, *n_);
        if (kx_.p >= 0 && n_->is_layer(kx_.p)) {
            fence();
            leafvalue_type entry(n_->lv_[kx_.p]);
            entry.layer()->prefetch_full();
            fence();
            if (likely(!v.deleted())
                && !n_->has_changed(oldv, old_perm)
                && !entry.layer()->has_split()) {
                ka_.shift();
                return entry.layer();
            }
        }

        // Otherwise lock.
        v = n_->lock(oldv, ti.lock_fence(tc_leaf_lock));

        // Maybe the old position works.
        if (likely(!v.deleted()) && !n_->has_changed(oldv, old_perm)) {
        found:
            if (kx_.p >= 0 && n_->is_layer(kx_.p)) {
                root = n_->lv_[kx_.p].layer();
                if (root->has_split())
                    n_->lv_[kx_.p] = root = root->unsplit_ancestor();
                n_->unlock(v);
                ka_.shift();
                return root;
            } else
                return 0;
        }
    }


    // Walk along leaves.
    while (1) {
        if (unlikely(v.deleted())) {
            n_->unlock(v);
            return root;
        }
        kx_ = leaf_type::bound_type::lower(ka_, *n_);
        if (kx_.p >= 0) {
            n_->lv_[kx_.p].prefetch(n_->keylenx_[kx_.p]);
            goto found;
        } else if (likely(kx_.i != n_->size())
                   || likely(!v.has_split(oldv))
                   || !(next = n_->safe_next())
                   || compare(ka_.ikey(), next->ikey_bound()) < 0)
            goto found;
        n_->unlock(v);
        ti.mark(tc_leaf_retry);
        ti.mark(tc_leaf_walk);
        do {
            n_ = next;
            oldv = n_->stable();
        } while (!unlikely(oldv.deleted())
                 && (next = n_->safe_next())
                 && compare(ka_.ikey(), next->ikey_bound()) >= 0);
        n_->prefetch();
        v = n_->lock(oldv, ti.lock_fence(tc_leaf_lock));
    }
}

template <typename P>
inline node_base<P>* tcursor<P>::check_leaf_locked(node_type* root,
                                                   nodeversion_type v,
                                                   threadinfo& ti)
{
    if (node_type* next_root = get_leaf_locked(root, v, ti))
        return next_root;
    if (kx_.p >= 0) {
        if (!n_->ksuf_equals(kx_.p, ka_))
            kx_.p = -1;
    } else if (kx_.i == 0 && unlikely(n_->deleted_layer())) {
        n_->unlock();
        return reset_retry();
    }
    return 0;
}

template <typename P>
bool tcursor<P>::find_locked(threadinfo& ti)
{
    nodeversion_type v;
    node_type* root = root_;
    while (1) {
        n_ = root->reach_leaf(ka_, v, ti);
        original_n_ = n_;
        original_v_ = n_->full_unlocked_version_value();

        root = check_leaf_locked(root, v, ti);
        if (!root) {
            state_ = kx_.p >= 0;
            return kx_.p >= 0;
        }
    }
}

} // namespace Masstree
#endif
