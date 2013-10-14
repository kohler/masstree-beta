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
#ifndef MASSTREE_TCURSOR_HH
#define MASSTREE_TCURSOR_HH 1
#include "masstree_key.hh"
#include "masstree_struct.hh"
namespace Masstree {
template <typename P> struct gc_layer_rcu_callback;

template <typename P>
class unlocked_tcursor {
  public:
    typedef typename P::value_type value_type;
    typedef key<typename P::ikey_type> key_type;
    typedef typename P::threadinfo_type threadinfo;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;
    typedef typename nodeversion_type::value_type nodeversion_value_type;

    inline unlocked_tcursor(const basic_table<P>& table, Str str)
        : ka_(str), lv_(leafvalue<P>::make_empty()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table, Str str)
        : ka_(str), lv_(leafvalue<P>::make_empty()),
          root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table,
                            const char* s, int len)
        : ka_(s, len), lv_(leafvalue<P>::make_empty()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table,
                            const char* s, int len)
        : ka_(s, len), lv_(leafvalue<P>::make_empty()),
          root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table,
                            const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len),
          lv_(leafvalue<P>::make_empty()), root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table,
                            const unsigned char* s, int len)
        : ka_(reinterpret_cast<const char*>(s), len),
          lv_(leafvalue<P>::make_empty()), root_(table.fix_root()) {
    }

    bool find_unlocked(threadinfo& ti);

    inline value_type value() const {
        return lv_.value();
    }
    inline leaf<P>* node() const {
        return n_;
    }
    inline nodeversion_value_type full_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
        return (v_.version_value() << leaf<P>::permuter_type::size_bits) + perm_.size();
    }

  private:
    leaf<P>* n_;
    key_type ka_;
    typename leaf<P>::nodeversion_type v_;
    typename leaf<P>::permuter_type perm_;
    leafvalue<P> lv_;
    const node_base<P>* root_;

    inline int lower_bound_binary() const;
    inline int lower_bound_linear() const;
};

template <typename P>
class tcursor {
  public:
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef internode<P> internode_type;
    typedef typename P::value_type value_type;
    typedef leafvalue<P> leafvalue_type;
    typedef typename leaf_type::permuter_type permuter_type;
    typedef typename P::ikey_type ikey_type;
    typedef key<ikey_type> key_type;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;
    typedef typename nodeversion_type::value_type nodeversion_value_type;
    typedef typename P::threadinfo_type threadinfo;

    tcursor(basic_table<P>& table, Str str)
	: ka_(str), root_(table.fix_root()) {
    }
    tcursor(basic_table<P>& table, const char* s, int len)
	: ka_(s, len), root_(table.fix_root()) {
    }
    tcursor(basic_table<P>& table, const unsigned char* s, int len)
	: ka_(reinterpret_cast<const char*>(s), len), root_(table.fix_root()) {
    }
    tcursor(node_base<P>* root, const char* s, int len)
	: ka_(s, len), root_(root) {
    }
    tcursor(node_base<P>* root, const unsigned char* s, int len)
	: ka_(reinterpret_cast<const char*>(s), len), root_(root) {
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

    inline leaf<P>* node() const {
        return n_;
    }
    inline kvtimestamp_t node_timestamp() const {
	return n_->node_ts_;
    }
    inline kvtimestamp_t &node_timestamp() {
	return n_->node_ts_;
    }

    inline bool find_locked(threadinfo& ti);
    inline bool find_insert(threadinfo& ti);

    inline void finish(int answer, threadinfo& ti);

    inline nodeversion_value_type previous_full_version_value() const;
    inline nodeversion_value_type next_full_version_value(int state) const;

  private:
    leaf_type *n_;
    key_type ka_;
    int ki_;
    int kp_;
    node_base<P>* root_;
    int state_;

    inline node_type* reset_retry() {
	ka_.unshift_all();
	return root_;
    }

    inline node_type* get_leaf_locked(node_type* root, nodeversion_type& v, threadinfo& ti);
    inline node_type* check_leaf_locked(node_type* root, nodeversion_type v, threadinfo& ti);
    inline node_type* check_leaf_insert(node_type* root, nodeversion_type v, threadinfo& ti);
    static inline node_type* insert_marker() {
	return reinterpret_cast<node_type*>(uintptr_t(1));
    }
    static inline node_type* found_marker() {
	return reinterpret_cast<node_type*>(uintptr_t(0));
    }

    node_type* finish_split(threadinfo& ti);
    inline void finish_insert();
    inline bool finish_remove(threadinfo& ti);

    static void collapse(internode_type* p, ikey_type ikey,
                         node_type* root, Str prefix, threadinfo& ti);
    /** Remove @a leaf from the Masstree rooted at @a rootp.
     * @param prefix String defining the path to the tree containing this leaf.
     *   If removing a leaf in layer 0, @a prefix is empty.
     *   If removing, for example, the node containing key "01234567ABCDEF" in the layer-1 tree
     *   rooted at "01234567", then @a prefix should equal "01234567". */
    static bool remove_leaf(leaf_type* leaf, node_type* root,
                            Str prefix, threadinfo& ti);

    bool gc_layer(threadinfo& ti);
    friend struct gc_layer_rcu_callback<P>;
};

template <typename P>
inline typename tcursor<P>::nodeversion_value_type
tcursor<P>::previous_full_version_value() const {
    static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
    return (n_->unlocked_version_value() << leaf<P>::permuter_type::size_bits) + n_->size();
}

template <typename P>
inline typename tcursor<P>::nodeversion_value_type
tcursor<P>::next_full_version_value(int state) const {
    static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
    typename node_base<P>::nodeversion_type v(*n_);
    v.unlock();
    nodeversion_value_type result = (v.version_value() << leaf<P>::permuter_type::size_bits) + n_->size();
    if (state < 0 && (state_ & 1))
        return result - 1;
    else if (state > 0 && state_ == 2)
        return result + 1;
    else
        return result;
}

} // namespace Masstree
#endif
