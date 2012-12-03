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
#ifndef MASSTREE_TCURSOR_HH
#define MASSTREE_TCURSOR_HH 1
#include "masstree.hh"
#include "masstree_key.hh"
namespace Masstree {
template <typename P> struct remove_layer_rcu_callback;

template <typename P>
struct unlocked_tcursor {
    typedef key<typename P::ikey_type> key_type;
    typename P::value_type datum_;
    bool find_unlocked(const node_base<P> *root, key_type &ka, threadinfo *ti);
};

template <typename P>
struct tcursor {
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef internode<P> internode_type;
    typedef typename P::value_type value_type;
    typedef leafvalue<P> leafvalue_type;
    typedef typename leaf_type::permuter_type permuter_type;
    typedef typename P::ikey_type ikey_type;
    typedef key<ikey_type> key_type;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;

    leaf_type *n_;
    key_type ka_;
    int ki_;
    int kp_;

    tcursor(const str &str)
	: ka_(str) {
    }
    tcursor(const char *s, int len)
	: ka_(s, len) {
    }

    inline bool has_value() const {
	return kp_ >= 0;
    }
    inline value_type &value() const {
	return n_->lv_[kp_].value();
    }

    inline bool is_first_layer() const {
	return !ka_.is_shifted();
    }

    inline void find_locked(node_type **rootp, threadinfo *ti);
    inline bool find_insert(node_type **rootp, threadinfo *ti);
    inline void finish_insert();
    inline bool finish_remove(node_type **rootp, threadinfo *ti);

    /** Remove @a leaf from the Masstree rooted at @a rootp.
     * @param prefix String defining the path to the tree containing this leaf.
     *   If removing a leaf in layer 0, @a prefix is empty.
     *   If removing, for example, the node containing key "01234567ABCDEF" in the layer-1 tree
     *   rooted at "01234567", then @a prefix should equal "01234567". */
    static bool remove_leaf(leaf_type *leaf, node_type **rootp, const str &prefix, threadinfo *ti);

  private:

    inline node_type *reset_retry(node_type **rootp) {
	ka_.unshift_all();
	return *rootp;
    }

    inline node_type *get_leaf_locked(node_type *root, nodeversion_type &v, threadinfo *ti);
    inline node_type *check_leaf_locked(node_type *root, nodeversion_type v,
                                        node_type **rootp, threadinfo *ti);
    inline node_type *check_leaf_insert(node_type *root, nodeversion_type v,
                                        node_type **rootp, threadinfo *ti);
    static inline node_type *insert_marker() {
	return reinterpret_cast<node_type *>(uintptr_t(1));
    }

    node_type *finish_split(node_type **rootp, threadinfo *ti);

    static void prune_twig(internode_type *p, ikey_type ikey,
			   node_type **rootp, const str &prefix, threadinfo *ti);
    bool remove_layer(node_type **rootp, threadinfo *ti);
    friend struct remove_layer_rcu_callback<P>;

};

}
#endif
