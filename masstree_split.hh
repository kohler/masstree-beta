/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012 President and Fellows of Harvard College
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file is
 * legally binding.
 */
#ifndef MASSTREE_SPLIT_HH
#define MASSTREE_SPLIT_HH 1
#include "masstree_tcursor.hh"
#include "btree_leaflink.hh"
namespace Masstree {

template <typename P>
typename P::ikey_type internode_split(internode<P> *nl, internode<P> *nr,
                                      int p, typename P::ikey_type ka,
                                      node_base<P> *value, int split_type)
{
    // B+tree internal node insertion.
    // Split nl, with items [0,T::width), into nl + nr, simultaneously
    // inserting "ka:value" at position "p" (0 <= p <= T::width).
    // The midpoint element of the result is stored in "split_ikey".

    // Let mid = ceil(T::width / 2). After the split, the key at
    // post-insertion position mid is stored in split_ikey. nl contains keys
    // [0,mid) and nr contains keys [mid+1,T::width+1).
    // If p < mid, then x goes into nl, pre-insertion item mid-1 goes into
    //   split_ikey, and the first element of nr is pre-insertion item mid.
    // If p == mid, then x goes into split_ikey and the first element of
    //   nr is pre-insertion item mid.
    // If p > mid, then x goes into nr, pre-insertion item mid goes into
    //   split_ikey, and the first element of nr is post-insertion item mid+1.
    assert(!nl->concurrent || nl->locked());

    nr->assign_version(*nl);
    nr->mark_nonroot();
    int mid = (split_type == 2 ? nl->width : (nl->width + 1) / 2);
    nr->nkeys_ = nl->width + 1 - (mid + 1); // == nl->width - mid

    typename internode<P>::ikey_type split_ikey;
    if (p < mid) {
	nr->child_[0] = nl->child_[mid];
	nr->shift_from(0, nl, mid, nl->width - mid);
	split_ikey = nl->ikey0_[mid - 1];
    } else if (p == mid) {
	nr->child_[0] = value;
	nr->shift_from(0, nl, mid, nl->width - mid);
	split_ikey = ka;
    } else {
	nr->child_[0] = nl->child_[mid + 1];
	nr->shift_from(0, nl, mid + 1, p - (mid + 1));
	nr->assign(p - (mid + 1), ka, value);
	nr->shift_from(p + 1 - (mid + 1), nl, p, nl->width - p);
	split_ikey = nl->ikey0_[mid];
    }

    for (int i = 0; i <= nr->nkeys_; ++i)
	nr->child_[i]->set_parent(nr);

    return split_ikey;
}


template <typename P>
typename P::ikey_type leaf_ikey(leaf<P> *nl,
                                const typename leaf<P>::permuter_type &perml,
				const typename leaf<P>::key_type &ka,
				int ka_i, int i)
{
    if (i < ka_i)
	return nl->ikey0_[perml[i]];
    else if (i == ka_i)
	return ka.ikey();
    else
	return nl->ikey0_[perml[i - 1]];
}

template <typename P>
int leaf_split(leaf<P> *nl, leaf<P> *nr,
	       int p, const typename leaf<P>::key_type &ka,
	       threadinfo *ti)
{
    // B+tree leaf insertion.
    // Split nl, with items [0,T::width), into nl + nr, simultaneously
    // inserting "ka:value" at position "p" (0 <= p <= T::width).

    // Let mid = floor(T::width / 2) + 1. After the split,
    // "nl" contains [0,mid) and "nr" contains [mid,T::width+1).
    // If p < mid, then x goes into nl, and the first element of nr
    //   will be former item (mid - 1).
    // If p >= mid, then x goes into nr.
    assert(!nl->concurrent || (nl->locked() && nr->locked()));
    assert(nl->nremoved_ == 0 && nl->size() >= nl->width - 1);

    int width = nl->size();	// == nl->width or nl->width - 1
    int mid = nl->width / 2 + 1;
    if (p == 0 && !nl->prev_)
	mid = 1;
    else if (p == width && !nl->next_.ptr)
	mid = width;

    // Never split apart keys with the same ikey0.
    typename leaf<P>::permuter_type perml(nl->permutation_);
    typename P::ikey_type mid_ikey = leaf_ikey(nl, perml, ka, p, mid);
    if (mid_ikey == leaf_ikey(nl, perml, ka, p, mid - 1)) {
	int midl = mid - 2, midr = mid + 1;
	while (1) {
	    if (midr <= width
		&& mid_ikey != leaf_ikey(nl, perml, ka, p, midr)) {
		mid = midr;
		break;
	    } else if (midl >= 0
		       && mid_ikey != leaf_ikey(nl, perml, ka, p, midl)) {
		mid = midl + 1;
		break;
	    }
	    --midl, ++midr;
	}
	assert(mid > 0 && mid <= width);
    }

    typename leaf<P>::permuter_type::value_type pv = perml.value_from(mid - (p < mid));
    for (int x = mid; x <= width; ++x)
	if (x == p)
	    nr->assign_initialize(x - mid, ka, ti);
	else {
	    nr->assign_initialize(x - mid, nl, pv & 15, ti);
	    pv >>= 4;
	}
    typename leaf<P>::permuter_type permr = leaf<P>::permuter_type::make_sorted(nr->width, width + 1 - mid);
    if (p >= mid)
	permr.remove_to_back(nr->width, p - mid);
    nr->permutation_ = permr.value();

    btree_leaflink<leaf<P> >::link_split(nl, nr);

    return p >= mid ? 1 + (mid == width) : 0;
}


template <typename P>
node_base<P> *tcursor<P>::finish_split(threadinfo *ti)
{
    // create and populate new leaf, then insert it into the tree
    leaf_type *nr = leaf_type::make(n_->ksuf_size(), n_->node_ts_, ti);
    nr->assign_version(*n_);
    int split_type = leaf_split(n_, nr, ki_, ka_, ti);
    split_ascend(n_, nr, nr->ikey_bound(), split_type, ti);

    // update old leaf
    permuter_type perml(n_->permutation_);
    int old_size = perml.size();
    perml.set_size(old_size - nr->size());
    // removed item, if any, must be @ perml.size()
    if (old_size != n_->width)
        perml.exchange(n_->width, perml.size(), n_->width - 1);
    n_->mark_split();
    n_->permutation_ = perml.value();
    if (split_type == 0) {
        kp_ = perml.back(n_->width);
        n_->assign(kp_, ka_, ti);
        nr->unlock();
    } else {
        ki_ = kp_ = ki_ - perml.size();
        n_->unlock();
        n_ = nr;
    }

    return insert_marker();
}

template <typename P>
void tcursor<P>::split_ascend(node_type *nl, node_type *nr,
                              ikey_type ikey_bound, int split_type,
                              threadinfo *ti)
{
    internode_type *p = nl->locked_parent(ti);
    if (!p) {
        internode_type *nn = internode_type::make(ti);
        nn->child_[0] = nl;
        nn->assign(0, ikey_bound, nr);
        nn->nkeys_ = 1;
        nn->parent_ = 0;
        nn->mark_root();
        fence();
        nl->set_parent(nn);
        if (is_first_layer())
            tablep_->root_ = nn;
    } else {
        int kp = internode_type::bound_type::upper(ikey_bound, *p);

        if (p->size() < p->width)
            p->mark_insert();
        else {
            internode_type *pr = internode_type::make(ti);
            ikey_type split_ikey = internode_split(p, pr, kp, ikey_bound, nr,
                                                   split_type);
            split_ascend(p, pr, split_ikey, split_type, ti);

            pr->unlock();
            p->mark_split();
            p->nkeys_ = p->width - pr->nkeys_;
            if (kp < p->nkeys_)
                --p->nkeys_;
            else
                goto after_insert;
        }

        p->shift_up(kp + 1, kp, p->size() - kp);
        p->assign(kp, ikey_bound, nr);
        fence();
        ++p->nkeys_;

    after_insert:
        p->unlock();
    }
}

} // namespace Masstree
#endif
