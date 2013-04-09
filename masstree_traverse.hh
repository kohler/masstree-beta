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
#ifndef MASSTREE_TRAVERSE_HH
#define MASSTREE_TRAVERSE_HH 1
#include "masstree_struct.hh"
namespace Masstree {

template <typename P>
static inline int stable_last_key_compare(const typename node_base<P>::key_type &ka,
					  const internode<P> &n,
					  typename internode<P>::nodeversion_type v,
					  threadinfo *ti)
{
    while (1) {
	int cmp = key_compare(ka, n, n.size() - 1);
	if (likely(!n.has_changed(v)))
	    return cmp;
	v = n.stable_annotated(ti->stable_fence());
    }
}

template <typename P>
static inline int stable_last_key_compare(const typename node_base<P>::key_type &ka,
					  const leaf<P> &n,
					  typename leaf<P>::nodeversion_type v,
					  threadinfo *ti)
{
    while (1) {
	typename leaf<P>::permuter_type perm(n.permutation_);
	int p = perm[perm.size() - 1];
	int cmp = key_compare(ka, n, p);
	if (likely(!n.has_changed(v)))
	    return cmp;
	v = n.stable_annotated(ti->stable_fence());
    }
}

template <typename P>
inline leaf<P> *reach_leaf(const node_base<P> *root,
			   const typename node_base<P>::key_type &ka,
			   threadinfo *ti,
			   typename node_base<P>::nodeversion_type &version)
{
    const node_base<P> *n[2];
    typename node_base<P>::nodeversion_type v[2];
    bool sense;

    // Get a non-stale root.
    // Detect staleness by checking whether n has ever split.
    // The true root has never split.
 retry:
    sense = false;
    n[sense] = root;
    while (1) {
	v[sense] = n[sense]->stable_annotated(ti->stable_fence());
	if (!v[sense].has_split())
	    break;
	ti->mark(tc_root_retry);
	n[sense] = n[sense]->unsplit_ancestor();
    }

    // Loop over internal nodes.
    while (!v[sense].isleaf()) {
	const internode<P> *in = static_cast<const internode<P> *>(n[sense]);
	in->prefetch();
	int kp = internode<P>::bound_type::upper(ka, *in);
	n[!sense] = in->child_[kp];
	if (!n[!sense])
	    goto retry;
	v[!sense] = n[!sense]->stable_annotated(ti->stable_fence());

	if (likely(!in->has_changed(v[sense]))) {
	    sense = !sense;
	    continue;
	}

	typename node_base<P>::nodeversion_type oldv = v[sense];
	v[sense] = in->stable_annotated(ti->stable_fence());
	if (oldv.has_split(v[sense])
	    && stable_last_key_compare(ka, *in, v[sense], ti) > 0) {
	    ti->mark(tc_root_retry);
	    goto retry;
	} else
	    ti->mark(tc_internode_retry);
    }

    version = v[sense];
    return const_cast<leaf<P> *>(static_cast<const leaf<P> *>(n[sense]));
}

template <typename P>
leaf<P> *forward_at_leaf(const leaf<P> *n,
			 typename leaf<P>::nodeversion_type &v,
			 const typename node_base<P>::key_type &ka,
			 threadinfo *ti)
{
    typename leaf<P>::nodeversion_type oldv = v;
    v = n->stable_annotated(ti->stable_fence());
    if (v.has_split(oldv)
	&& stable_last_key_compare(ka, *n, v, ti) > 0) {
	leaf<P> *next;
	ti->mark(tc_leaf_walk);
	while (likely(!v.deleted()) && (next = n->safe_next())
	       && compare(ka.ikey(), next->ikey_bound()) >= 0) {
	    n = next;
	    v = n->stable_annotated(ti->stable_fence());
	}
    }
    return const_cast<leaf<P> *>(n);
}

template <typename P>
internode<P> *node_base<P>::locked_parent(threadinfo *ti) const
{
    const node_base<P> *n = this;
    precondition(!n->concurrent || n->locked());
    while (1) {
	node_base<P> *p = n->parent();
	if (!p)
	    return 0;
	nodeversion_type pv = p->lock(*p, ti->lock_fence(tc_internode_lock));
	if (p == n->parent()) {
	    invariant(!p->isleaf());
	    return static_cast<internode<P> *>(p);
	}
	p->unlock(pv);
	relax_fence();
    }
}

} // namespace Masstree
#endif
