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
#include "OLFIT.hh"
#ifndef OLFIT_TCURSOR_HH
#define OLFIT_TCURSOR_HH 1
#include "../small_vector.hh"
#include "../fixsizedkey.hh"
#include "OLFIT_struct.hh"

namespace OLFIT {
template <typename P> struct gc_layer_rcu_callback;

template <typename P>
class unlocked_tcursor {
  public:
    using value_type = typename P::value_type;
    using key_type = fix_sized_key<P::ikey_size>;
    typedef typename P::threadinfo_type threadinfo;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;
    typedef typename nodeversion_type::value_type nodeversion_value_type;
    typedef typename leaf<P>::permuter_type permuter_type;

    inline unlocked_tcursor(const basic_table<P>& table, Str str)
        : k_(str), value_(value_type()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table, Str str)
        : k_(str), value_(value_type()),
          root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table,
                            const char* s, int len)
        : k_(s, len), value_(value_type()),
          root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table,
                            const char* s, int len)
        : k_(s, len), value_(value_type()),
          root_(table.fix_root()) {
    }
    inline unlocked_tcursor(const basic_table<P>& table,
                            const unsigned char* s, int len)
        : k_(reinterpret_cast<const char*>(s), len),
          value_(value_type()), root_(table.root()) {
    }
    inline unlocked_tcursor(basic_table<P>& table,
                            const unsigned char* s, int len)
        : k_(reinterpret_cast<const char*>(s), len),
          value_(value_type()), root_(table.fix_root()) {
    }

    bool find_unlocked(threadinfo& ti);

    inline value_type value() const {
        return value_;
    }
    inline leaf<P>* node() const {
        return n_;
    }
    inline permuter_type permutation() const {
        return perm_;
    }
    inline int compare_key(const key_type& a, int bp) const {
        return n_->compare_key(a, bp);
    }
    inline nodeversion_value_type full_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(leaf<P>::permuter_type::size_bits), "not enough bits to add size to version");
        return (v_.version_value() << leaf<P>::permuter_type::size_bits) + perm_.size();
    }

  private:
    leaf<P>* n_;
    key_type k_;
    typename leaf<P>::nodeversion_type v_;
    permuter_type perm_;
    value_type value_;
    const node_base<P>* root_;
};

template <typename P>
class tcursor {
  public:
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef internode<P> internode_type;
    typedef typename P::value_type value_type;
    typedef typename leaf_type::permuter_type permuter_type;
    typedef fix_sized_key<P::ikey_size> key_type;
    typedef typename leaf<P>::nodeversion_type nodeversion_type;
    typedef typename nodeversion_type::value_type nodeversion_value_type;
    typedef typename P::threadinfo_type threadinfo;
    static constexpr int new_nodes_size = 1; // unless we make a new trie newnodes will have at most 1 item
    typedef small_vector<std::pair<leaf_type*, nodeversion_value_type>, new_nodes_size> new_nodes_type;

    tcursor(basic_table<P>& table, Str str)
        : k_(str), root_(table.fix_root()) {
    }
    tcursor(basic_table<P>& table, const char* s, int len)
        : k_(s, len), root_(table.fix_root()) {
    }
    tcursor(basic_table<P>& table, const unsigned char* s, int len)
        : k_(reinterpret_cast<const char*>(s), len), root_(table.fix_root()) {
    }
    tcursor(node_base<P>* root, const char* s, int len)
        : k_(s, len), root_(root) {
    }
    tcursor(node_base<P>* root, const unsigned char* s, int len)
        : k_(reinterpret_cast<const char*>(s), len), root_(root) {
    }

    inline bool has_value() const {
        return kx_.p >= 0;
    }
    inline value_type& value() const {
        return n_->lv_[kx_.p].value();
    }

    inline leaf<P>* node() const {
        return n_;
    }

    inline leaf_type* original_node() const {
        return original_n_;
    }

    inline nodeversion_value_type original_version_value() const {
        return original_v_;
    }

    inline nodeversion_value_type updated_version_value() const {
        return updated_v_;
    }

    inline const new_nodes_type &new_nodes() const {
        return new_nodes_;
    }

    inline bool find_locked(threadinfo& ti);
    inline bool find_insert(threadinfo& ti);

    inline void finish(int answer, threadinfo& ti);

    inline nodeversion_value_type previous_full_version_value() const;
    inline nodeversion_value_type next_full_version_value(int state) const;

  private:
    leaf_type* n_;
    key_type k_;
    key_indexed_position kx_;
    node_base<P>* root_;
    int state_;

    leaf_type* original_n_;
    nodeversion_value_type original_v_;
    nodeversion_value_type updated_v_;
    new_nodes_type new_nodes_;

    // inline node_type* reset_retry() {
    //     ka_.unshift_all();
    //     return root_;
    // }

    // bool make_new_layer(threadinfo& ti);
    bool make_split(threadinfo& ti);
    friend class leaf<P>;
    inline void finish_insert();
    inline bool finish_remove(threadinfo& ti);

    static void redirect(internode_type* n, key_type ikey,
                         key_type replacement, threadinfo& ti);
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
