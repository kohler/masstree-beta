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
template <typename N> struct remove_layer_rcu_callback;

template <typename N>
struct unlocked_tcursor {
    typename N::value_type datum_;
    bool find_unlocked(N *root, typename N::key_type &ka, threadinfo *ti);
};

template <typename N>
struct tcursor {
    typedef typename N::internode_type internode_type;
    typedef typename N::leaf_type leaf_type;
    typedef typename N::value_type value_type;
    typedef typename N::leafvalue_type leafvalue_type;
    typedef typename N::leaf_type::permuter_type permuter_type;
    typedef typename N::key_type key_type;
    typedef typename N::ikey_type ikey_type;
    typedef typename N::nodeversion_type nodeversion_type;

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

    inline void find_locked(N **rootp, threadinfo *ti);
    inline bool find_insert(N **rootp, threadinfo *ti);
    inline void finish_insert();
    inline bool finish_remove(N **rootp, threadinfo *ti);

    /** Remove @a leaf from the Masstree rooted at @a rootp.
     * @param prefix String defining the path to the tree containing this leaf.
     *   If removing a leaf in layer 0, @a prefix is empty.
     *   If removing, for example, the node containing key "01234567ABCDEF" in the layer-1 tree
     *   rooted at "01234567", then @a prefix should equal "01234567". */
    static bool remove_leaf(leaf_type *leaf, N **rootp, const str &prefix, threadinfo *ti);

  private:

    inline N *reset_retry(N **rootp) {
	ka_.unshift_all();
	return *rootp;
    }

    inline N *get_leaf_locked(N *root, nodeversion_type &v, threadinfo *ti);
    inline N *check_leaf_locked(N *root, nodeversion_type v,
				N **rootp, threadinfo *ti);
    inline N *check_leaf_insert(N *root, nodeversion_type v,
				N **rootp, threadinfo *ti);
    static inline N *insert_marker() {
	return reinterpret_cast<N *>(uintptr_t(1));
    }

    N *finish_split(N **rootp, threadinfo *ti);

    static void prune_twig(internode_type *p, typename N::ikey_type ikey,
			   N **rootp, const str &prefix, threadinfo *ti);
    bool remove_layer(N **rootp, threadinfo *ti);
    friend struct remove_layer_rcu_callback<N>;

};

}
#endif
