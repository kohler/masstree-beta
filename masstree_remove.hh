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
#ifndef MASSTREE_REMOVE_HH
#define MASSTREE_REMOVE_HH
#include "masstree_get.hh"
#include "btree_leaflink.hh"
#include "circular_int.hh"
namespace Masstree {

template <typename P>
bool tcursor<P>::gc_layer(threadinfo& ti)
{
    find_locked(ti);
    masstree_precondition(!n_->deleted() && !n_->deleted_layer());

    // find_locked might return early if another gc_layer attempt has
    // succeeded at removing multiple tree layers. So check that the whole
    // key has been consumed
    if (ka_.has_suffix())
	return false;

    // find the slot for the child tree
    // ka_ is a multiple of ikey_size bytes long. We are looking for the entry
    // for the next tree layer, which has keylenx_ corresponding to ikey_size+1.
    // So if has_value(), then we found an entry for the same ikey, but with
    // length ikey_size; we need to adjust ki_.
    ki_ += has_value();
    if (ki_ >= n_->size())
	return false;
    permuter_type perm(n_->permutation_);
    kp_ = perm[ki_];
    if (n_->ikey0_[kp_] != ka_.ikey() || !n_->value_is_stable_layer(kp_))
	return false;

    // remove redundant internode layers
    node_type *layer;
    while (1) {
	layer = n_->lv_[kp_].layer();
	if (layer->has_split())
	    n_->lv_[kp_] = layer = layer->unsplit_ancestor();
	if (layer->isleaf())
	    break;

	internode_type *in = static_cast<internode_type *>(layer);
	if (in->size() > 0 && !in->has_split())
	    return false;
	in->lock(*in, ti.lock_fence(tc_internode_lock));
	if (in->has_split() && !in->has_parent())
	    in->mark_root();
	if (in->size() > 0 || in->has_split()) {
	    in->unlock();
	    return false;
	}

	node_type *child = in->child_[0];
	child->set_parent(node_type::parent_for_layer_root(n_));
	n_->lv_[kp_] = child;
	in->mark_split();
	in->set_parent(child);	// ensure concurrent reader finds true root
				// NB: now p->parent() might weirdly be a LEAF!
	in->unlock();
	in->deallocate_rcu(ti);
    }

    // we are left with a leaf child
    leaf_type *lf = static_cast<leaf_type *>(layer);
    if (lf->size() > 0 && !lf->has_split())
	return false;
    lf->lock(*lf, ti.lock_fence(tc_leaf_lock));
    if (lf->has_split() && !lf->has_parent())
	lf->mark_root();
    if (lf->size() > 0 || lf->has_split()) {
	lf->unlock();
	return false;
    }

    // child is an empty leaf: kill it
    masstree_invariant(!lf->prev_ && !lf->next_.ptr);
    masstree_invariant(!lf->deleted());
    masstree_invariant(!lf->deleted_layer());
    if (circular_int<kvtimestamp_t>::less(n_->node_ts_, lf->node_ts_))
	n_->node_ts_ = lf->node_ts_;
    lf->mark_deleted_layer();	// NB DO NOT mark as deleted (see above)
    lf->unlock();
    lf->deallocate_rcu(ti);
    return true;
}

template <typename P>
struct gc_layer_rcu_callback : public P::threadinfo_type::rcu_callback {
    typedef typename P::threadinfo_type threadinfo;
    node_base<P>* root_;
    int len_;
    char s_[0];
    gc_layer_rcu_callback(node_base<P>* root, Str prefix)
        : root_(root), len_(prefix.length()) {
        memcpy(s_, prefix.data(), len_);
    }
    void operator()(threadinfo& ti);
    size_t size() const {
	return len_ + sizeof(*this);
    }
    static void make(node_base<P>* root, Str prefix, threadinfo& ti);
};

template <typename P>
void gc_layer_rcu_callback<P>::operator()(threadinfo& ti)
{
    root_ = root_->unsplit_ancestor();
    if (!root_->deleted()) {    // if not destroying tree...
        tcursor<P> lp(root_, s_, len_);
        bool do_remove = lp.gc_layer(ti);
        if (!do_remove || !lp.finish_remove(ti))
            lp.n_->unlock();
        ti.deallocate(this, size(), memtag_masstree_gc);
    }
}

template <typename P>
void gc_layer_rcu_callback<P>::make(node_base<P>* root, Str prefix,
                                    threadinfo& ti)
{
    size_t sz = prefix.len + sizeof(gc_layer_rcu_callback<P>);
    void *data = ti.allocate(sz, memtag_masstree_gc);
    gc_layer_rcu_callback<P> *cb =
        new(data) gc_layer_rcu_callback<P>(root, prefix);
    ti.rcu_register(cb);
}

template <typename P>
bool tcursor<P>::finish_remove(threadinfo& ti)
{
    permuter_type perm(n_->permutation_);
    perm.remove(ki_);
    n_->permutation_ = perm.value();
    ++n_->nremoved_;
    if (perm.size())
	return false;
    else
	return remove_leaf(n_, root_, ka_.prefix_string(), ti);
}

template <typename P>
bool tcursor<P>::remove_leaf(leaf_type* leaf, node_type* root,
                             Str prefix, threadinfo& ti)
{
    if (!leaf->prev_) {
	if (!leaf->next_.ptr && !prefix.empty())
	    gc_layer_rcu_callback<P>::make(root, prefix, ti);
	return false;
    }

    // mark leaf deleted, RCU-free
    leaf->mark_deleted();
    leaf->deallocate_rcu(ti);

    // Ensure node that becomes responsible for our keys has its node_ts_ kept
    // up to date
    while (1) {
	leaf_type *prev = leaf->prev_;
	kvtimestamp_t prev_ts = prev->node_ts_;
	while (circular_int<kvtimestamp_t>::less(prev_ts, leaf->node_ts_)
	       && !bool_cmpxchg(&prev->node_ts_, prev_ts, leaf->node_ts_))
	    prev_ts = prev->node_ts_;
	fence();
	if (prev == leaf->prev_)
	    break;
    }

    // Unlink leaf from doubly-linked leaf list
    btree_leaflink<leaf_type>::unlink(leaf);

    // Remove leaf from tree. This is simple unless the leaf is the first
    // child of its parent, in which case we need to traverse up until we find
    // its key.
    node_type *n = leaf;
    ikey_type ikey = leaf->ikey_bound(), reshape_ikey = 0;
    bool reshaping = false;

    while (1) {
	internode_type *p = n->locked_parent(ti);
	masstree_invariant(p);
	n->unlock();

	int kp = internode_type::bound_type::upper(ikey, *p);
	masstree_invariant(kp == 0 || key_compare(ikey, *p, kp - 1) == 0);

	if (kp > 0) {
	    p->mark_insert();
	    if (!reshaping) {
		p->shift_down(kp - 1, kp, p->nkeys_ - kp);
		--p->nkeys_;
	    } else
		p->ikey0_[kp - 1] = reshape_ikey;
	    if (kp > 1 || p->child_[0]) {
		if (p->size() == 0)
		    collapse(p, ikey, root, prefix, ti);
		else
		    p->unlock();
		break;
	    }
	}

	if (!reshaping) {
	    if (p->size() == 0) {
		p->mark_deleted();
		p->deallocate_rcu(ti);
	    } else {
		reshaping = true;
		reshape_ikey = p->ikey0_[0];
		p->child_[0] = 0;
	    }
	}

	n = p;
    }

    return true;
}

template <typename P>
void tcursor<P>::collapse(internode_type* p, ikey_type ikey,
                          node_type* root, Str prefix, threadinfo& ti)
{
    masstree_precondition(p && p->locked());

    while (1) {
	internode_type *gp = p->locked_parent(ti);
	if (!internode_type::parent_exists(gp)) {
	    if (!prefix.empty())
		gc_layer_rcu_callback<P>::make(root, prefix, ti);
	    p->unlock();
	    break;
	}

	int kp = key_upper_bound(ikey, *gp);
	masstree_invariant(gp->child_[kp] == p);
	gp->child_[kp] = p->child_[0];
	p->child_[0]->set_parent(gp);

	p->mark_deleted();
	p->unlock();
	p->deallocate_rcu(ti);

	p = gp;
	if (p->size() != 0) {
	    p->unlock();
	    break;
	}
    }
}

template <typename P>
struct destroy_rcu_callback : public P::threadinfo_type::rcu_callback {
    typedef typename P::threadinfo_type threadinfo;
    typedef typename node_base<P>::leaf_type leaf_type;
    typedef typename node_base<P>::internode_type internode_type;
    node_base<P>* root_;
    int count_;
    destroy_rcu_callback(node_base<P>* root)
        : root_(root), count_(0) {
    }
    void operator()(threadinfo& ti);
    static void make(node_base<P>* root, Str prefix, threadinfo& ti);
  private:
    static inline node_base<P>** link_ptr(node_base<P>* n);
    static inline void enqueue(node_base<P>* n, node_base<P>**& tailp);
};

template <typename P>
inline node_base<P>** destroy_rcu_callback<P>::link_ptr(node_base<P>* n) {
    if (n->isleaf())
        return &static_cast<leaf_type*>(n)->parent_;
    else
        return &static_cast<internode_type*>(n)->parent_;
}

template <typename P>
inline void destroy_rcu_callback<P>::enqueue(node_base<P>* n,
                                             node_base<P>**& tailp) {
    *tailp = n;
    tailp = link_ptr(n);
}

template <typename P>
void destroy_rcu_callback<P>::operator()(threadinfo& ti) {
    if (++count_ == 1) {
        root_ = root_->unsplit_ancestor();
        root_->lock();
        root_->mark_deleted_tree(); // i.e., deleted but not splitting
        root_->unlock();
        ti.rcu_register(this);
        return;
    }

    node_base<P>* workq;
    node_base<P>** tailp = &workq;
    enqueue(root_, tailp);

    while (node_base<P>* n = workq) {
        node_base<P>** linkp = link_ptr(n);
        if (linkp != tailp)
            workq = *linkp;
        else {
            workq = 0;
            tailp = &workq;
        }

        if (n->isleaf()) {
            leaf_type* l = static_cast<leaf_type*>(n);
            typename leaf_type::permuter_type perm = l->permutation();
            for (int i = 0; i != l->size(); ++i) {
                int p = perm[i];
                if (l->value_is_layer(p))
                    enqueue(l->lv_[p].layer(), tailp);
            }
            l->deallocate(ti);
        } else {
            internode_type* in = static_cast<internode_type*>(n);
            for (int i = 0; i != in->size() + 1; ++i)
                if (in->child_[i])
                    enqueue(in->child_[i], tailp);
            in->deallocate(ti);
        }
    }
    ti.deallocate(this, sizeof(this), memtag_masstree_gc);
}

template <typename P>
void basic_table<P>::destroy(threadinfo& ti) {
    if (root_) {
        void* data = ti.allocate(sizeof(destroy_rcu_callback<P>), memtag_masstree_gc);
        destroy_rcu_callback<P>* cb = new(data) destroy_rcu_callback<P>(root_);
        ti.rcu_register(cb);
        root_ = 0;
    }
}

} // namespace Masstree
#endif
