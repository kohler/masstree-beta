/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2016 President and Fellows of Harvard College
 * Copyright (c) 2012-2016 Massachusetts Institute of Technology
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
#ifndef OLFIT_STRUCT_HH
#define OLFIT_STRUCT_HH
#include "OLFIT.hh"
#include "../kvthread.hh"
#include "../nodeversion.hh"
#include "../stringbag.hh"
#include "../mtcounters.hh"
#include "../timestamp.hh"
namespace OLFIT {

template <typename P>
struct make_nodeversion {
    typedef nodeversion_parameters<typename P::nodeversion_value_type> parameters_type;
    typedef typename mass::conditional<P::concurrent,
                                       nodeversion<parameters_type>,
                                       singlethreaded_nodeversion<parameters_type> >::type type;
};

template <typename P>
struct make_prefetcher {
    typedef typename mass::conditional<P::prefetch,
                                       value_prefetcher<typename P::value_type>,
                                       do_nothing>::type type;
};

template <typename P>
class node_base : public make_nodeversion<P>::type {
  public:
    static constexpr bool concurrent = P::concurrent;
    static constexpr int nikey = 1;
    using leaf_type = leaf<P>;
    using internode_type = internode<P>;
    using base_type = node_base<P>;
    using ikey_type = fix_sized_key<P::ikey_size>;
    using value_type = typename P::value_type;
    using nodeversion_type = typename make_nodeversion<P>::type;
    using threadinfo = typename P::threadinfo_type;

    node_base(bool isleaf)
        : nodeversion_type(isleaf) {
    }

    inline base_type* parent() const {
        // almost always an internode
        if (this->isleaf())
            return static_cast<const leaf_type*>(this)->parent_;
        else
            return static_cast<const internode_type*>(this)->parent_;
    }
    inline bool parent_exists(base_type* p) const {
        return p != nullptr;
    }
    inline bool has_parent() const {
        return parent_exists(parent());
    }
    inline internode_type* locked_parent(threadinfo& ti) const;
    inline void set_parent(base_type* p) {
        if (this->isleaf())
            static_cast<leaf_type*>(this)->parent_ = p;
        else
            static_cast<internode_type*>(this)->parent_ = p;
    }

    inline base_type* maybe_parent() const {
        base_type* x = parent();
        return parent_exists(x) ? x : const_cast<base_type*>(this);
    }

    inline leaf_type* reach_leaf(const ikey_type& k, nodeversion_type& version,
                                 threadinfo& ti) const;

    void prefetch_full() const {
        for (int i = 0; i < std::min(16 * P::fanout + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
    }

    void print(FILE* f, const char* prefix, int depth, int kdepth) const;
};

template <typename P>
class internode : public node_base<P> {
  public:
    static constexpr int width = P::fanout;
    using nodeversion_type = typename node_base<P>::nodeversion_type;
    using ikey_type = fix_sized_key<P::ikey_size>;
    using bound_type = typename key_bound<width, P::bound_method>::type;
    using threadinfo = typename P::threadinfo_type;

    uint8_t nkeys_;
    uint32_t height_;
    ikey_type ikey0_[width];
    node_base<P>* child_[width + 1];
    node_base<P>* parent_;
    kvtimestamp_t created_at_[P::debug_level > 0];

    internode(uint32_t height)
        : node_base<P>(false), nkeys_(0), height_(height), parent_() {
    }

    static internode<P>* make(uint32_t height, threadinfo& ti) {
        void* ptr = ti.pool_allocate(sizeof(internode<P>),
                                     memtag_masstree_internode);
        internode<P>* n = new(ptr) internode<P>(height);
        assert(n);
        if (P::debug_level > 0)
            n->created_at_[0] = ti.operation_timestamp();
        return n;
    }

    int size() const {
        return nkeys_;
    }

    ikey_type ikey(int p) const {
        return ikey0_[p];
    }

    int compare_key(ikey_type a, int bp) const {
        return a.compare(ikey(bp));
    }
    int compare_key(const ikey_type& a, int bp) const {
        return a.compare(ikey(bp));
    }
    inline int stable_last_key_compare(const ikey_type& k, nodeversion_type v,
                                       threadinfo& ti) const;

    void prefetch() const {
        for (int i = 64; i < std::min(16 * width + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
    }

    void print(FILE* f, const char* prefix, int depth, int kdepth) const;

    void deallocate(threadinfo& ti) {
        ti.pool_deallocate(this, sizeof(*this), memtag_masstree_internode);
    }
    void deallocate_rcu(threadinfo& ti) {
        ti.pool_deallocate_rcu(this, sizeof(*this), memtag_masstree_internode);
    }

  private:
    void assign(int p, ikey_type ikey, node_base<P>* child) {
        child->set_parent(this);
        child_[p + 1] = child;
        ikey0_[p] = ikey;
    }

    void shift_from(int p, const internode<P>* x, int xp, int n) {
        masstree_precondition(x != this);
        if (n) {
            memcpy(ikey0_ + p, x->ikey0_ + xp, sizeof(ikey0_[0]) * n);
            memcpy(child_ + p + 1, x->child_ + xp + 1, sizeof(child_[0]) * n);
        }
    }
    void shift_up(int p, int xp, int n) {
        memmove(ikey0_ + p, ikey0_ + xp, sizeof(ikey0_[0]) * n);
        for (node_base<P> **a = child_ + p + n, **b = child_ + xp + n; n; --a, --b, --n)
            *a = *b;
    }
    void shift_down(int p, int xp, int n) {
        memmove(ikey0_ + p, ikey0_ + xp, sizeof(ikey0_[0]) * n);
        for (node_base<P> **a = child_ + p + 1, **b = child_ + xp + 1; n; ++a, ++b, --n)
            *a = *b;
    }

    int split_into(internode<P>* nr, int p, ikey_type ka, node_base<P>* value,
                   ikey_type& split_ikey, int split_type);

    template <typename PP> friend class tcursor;
};

template <typename P>
class leaf : public node_base<P> {
  public:
    static constexpr int width = P::fanout;
    using nodeversion_type = typename node_base<P>::nodeversion_type;
    using ikey_type = fix_sized_key<P::ikey_size>;
    using value_type = typename node_base<P>::value_type;
    using permuter_type = kpermuter<P::fanout>;
    using bound_type = typename key_bound<width, P::bound_method>::type;
    using threadinfo = typename P::threadinfo_type;
    using phantom_epoch_type = typename P::phantom_epoch_type;

    enum {
        modstate_insert = 0, modstate_remove = 1, modstate_deleted_layer = 2
    };

    uint8_t modstate_;
    typename permuter_type::storage_type permutation_;

    ikey_type ikey0_[width];

    value_type v_[width];
    union {
        leaf<P>* ptr;
        uintptr_t x;
    } next_;
    leaf<P>* prev_;
    node_base<P>* parent_;
    phantom_epoch_type phantom_epoch_[P::need_phantom_epoch];
    kvtimestamp_t created_at_[P::debug_level > 0];

    leaf(size_t sz, phantom_epoch_type phantom_epoch)
        : node_base<P>(true), modstate_(modstate_insert),
          permutation_(permuter_type::make_empty()),
          parent_(){
        masstree_precondition(sz % 64 == 0 && sz / 64 < 128);
        if (P::need_phantom_epoch) {
            phantom_epoch_[0] = phantom_epoch;
        }
    }

    static leaf<P>* make(int ksufsize, phantom_epoch_type phantom_epoch, threadinfo& ti) {
        size_t sz = iceil(sizeof(leaf<P>) + std::min(ksufsize, 128), 64);
        void* ptr = ti.pool_allocate(sz, memtag_masstree_leaf);
        leaf<P>* n = new(ptr) leaf<P>(sz, phantom_epoch);
        assert(n);
        if (P::debug_level > 0) {
            n->created_at_[0] = ti.operation_timestamp();
        }
        return n;
    }
    static leaf<P>* make_root(int ksufsize, leaf<P>* parent, threadinfo& ti) {
        leaf<P>* n = make(ksufsize, parent ? parent->phantom_epoch() : phantom_epoch_type(), ti);
        n->next_.ptr = n->prev_ = 0;
        n->ikey0_[0] = ikey_type(); // to avoid undefined behavior
        n->set_parent(nullptr);
        n->mark_root();
        return n;
    }

    static size_t min_allocated_size() {
        return (sizeof(leaf<P>) + 63) & ~size_t(63);
    }
    size_t allocated_size() const {
        return sizeof(*this);
    }
    phantom_epoch_type phantom_epoch() const {
        return P::need_phantom_epoch ? phantom_epoch_[0] : phantom_epoch_type();
    }

    int size() const {
        return permuter_type::size(permutation_);
    }
    permuter_type permutation() const {
        return permuter_type(permutation_);
    }
    typename nodeversion_type::value_type full_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(permuter_type::size_bits), "not enough bits to add size to version");
        return (this->version_value() << permuter_type::size_bits) + size();
    }
    typename nodeversion_type::value_type full_unlocked_version_value() const {
        static_assert(int(nodeversion_type::traits_type::top_stable_bits) >= int(permuter_type::size_bits), "not enough bits to add size to version");
        typename node_base<P>::nodeversion_type v(*this);
        if (v.locked()) {
            // subtly, unlocked_version_value() is different than v.unlock();
            // v.version_value() because the latter will add a split bit if
            // we're doing a split. So we do the latter to get the fully
            // correct version.
            v.unlock();
        }
        return (v.version_value() << permuter_type::size_bits) + size();
    }

    using node_base<P>::has_changed;
    bool has_changed(nodeversion_type oldv,
                     typename permuter_type::storage_type oldperm) const {
        return this->has_changed(oldv) || oldperm != permutation_;
    }

    ikey_type ikey(int p) const {
        return ikey0_[p];
    }
    ikey_type ikey_bound() const {
        return ikey0_[0];
    }
    int compare_key(const ikey_type& a, int bp) const {
        return a.compare(ikey(bp));
    }
    inline int stable_last_key_compare(const ikey_type& k, nodeversion_type v,
                                       threadinfo& ti) const;

    inline leaf<P>* advance_to_key(const ikey_type& k, nodeversion_type& version,
                                   threadinfo& ti) const;

    bool deleted_leaf() const {
        return modstate_ == modstate_deleted_layer;
    }

    void prefetch() const {
        for (int i = 64; i < std::min(16 * width + 1, 4 * 64); i += 64)
            ::prefetch((const char *) this + i);
    }

    void print(FILE* f, const char* prefix, int depth, int kdepth) const;

    leaf<P>* safe_next() const {
        return reinterpret_cast<leaf<P>*>(next_.x & ~(uintptr_t) 1);
    }

    void deallocate(threadinfo& ti) {
        ti.pool_deallocate(this, allocated_size(), memtag_masstree_leaf);
    }
    void deallocate_rcu(threadinfo& ti) {
        ti.pool_deallocate_rcu(this, allocated_size(), memtag_masstree_leaf);
    }

  private:
    inline void mark_deleted_leaf() {
        modstate_ = modstate_deleted_layer;
    }

    // inline void assign(int p, const ikey_type& ka, threadinfo& ti) {
    //     v_[p] = value_type();
    //     ikey0_[p] = ka;
    // }

    inline ikey_type ikey_after_insert(const permuter_type& perm, int i,
                                       const tcursor<P>* cursor) const;
    int split_into(leaf<P>* nr, tcursor<P>* tcursor, ikey_type& split_ikey,
                   threadinfo& ti);

    template <typename PP> friend class tcursor;
};


template <typename P>
void basic_table<P>::initialize(threadinfo& ti) {
    masstree_precondition(!root_);
    root_ = node_type::leaf_type::make_root(0, 0, ti);
}


/** @brief Return this node's parent in locked state.
    @pre this->locked()
    @post this->parent() == result && (!result || result->locked()) */
template <typename P>
internode<P>* node_base<P>::locked_parent(threadinfo& ti) const
{
    node_base<P>* p;
    masstree_precondition(!this->concurrent || this->locked());
    while (true) {
        p = this->parent();
        if (!this->parent_exists(p)) {
            break;
        }
        nodeversion_type pv = p->lock(*p, ti.lock_fence(tc_internode_lock));
        if (p == this->parent()) {
            masstree_invariant(!p->isleaf());
            break;
        }
        p->unlock(pv);
        relax_fence();
    }
    return static_cast<internode<P>*>(p);
}


template <typename P>
void node_base<P>::print(FILE* f, const char* prefix, int depth, int kdepth) const
{
    if (this->isleaf())
        static_cast<const leaf<P>*>(this)->print(f, prefix, depth, kdepth);
    else
        static_cast<const internode<P>*>(this)->print(f, prefix, depth, kdepth);
}


/** @brief Return the result of compare_key(k, LAST KEY IN NODE).

    Reruns the comparison until a stable comparison is obtained. */
template <typename P>
inline int
internode<P>::stable_last_key_compare(const ikey_type& k, nodeversion_type v,
                                      threadinfo& ti) const
{
    while (true) {
        int n = this->size();
        int cmp = n ? compare_key(k, n - 1) : 1;
        if (likely(!this->has_changed(v))) {
            return cmp;
        }
        v = this->stable_annotated(ti.stable_fence());
    }
}

template <typename P>
inline int
leaf<P>::stable_last_key_compare(const ikey_type& k, nodeversion_type v,
                                 threadinfo& ti) const
{
    while (true) {
        typename leaf<P>::permuter_type perm(permutation_);
        int n = perm.size();
        // If `n == 0`, then this node is empty: it was deleted without ever
        // splitting, or it split and then was emptied.
        // - It is always safe to return 1, because then the caller will
        //   check more precisely whether `k` belongs in `this`.
        // - It is safe to return anything if `this->deleted()`, because
        //   viewing the deleted node will always cause a retry.
        // - Thus it is safe to return a comparison with the key stored in slot
        //   `perm[0]`. If the node ever had keys in it, then kpermuter ensures
        //   that slot holds the most recently deleted key, which would belong
        //   in this leaf. Otherwise, `perm[0]` is 0.
        int p = perm[n ? n - 1 : 0];
        int cmp = compare_key(k, p);
        if (likely(!this->has_changed(v))) {
            return cmp;
        }
        v = this->stable_annotated(ti.stable_fence());
    }
}


/** @brief Return the leaf in this tree layer responsible for @a ka.

    Returns a stable leaf. Sets @a version to the stable version. */
template <typename P>
inline leaf<P>* node_base<P>::reach_leaf(const ikey_type& ka,
                                         nodeversion_type& version,
                                         threadinfo& ti) const
{
    const node_base<P> *n[2];
    typename node_base<P>::nodeversion_type v[2];
    unsigned sense;

    // Get a non-stale root.
    // Detect staleness by checking whether n has ever split.
    // The true root has never split.
 retry:
    sense = 0;
    n[sense] = this;
    while (true) {
        v[sense] = n[sense]->stable_annotated(ti.stable_fence());
        if (v[sense].is_root()) {
            break;
        }
        ti.mark(tc_root_retry);
        n[sense] = n[sense]->maybe_parent();
    }

    // Loop over internal nodes.
    while (!v[sense].isleaf()) {
        const internode<P> *in = static_cast<const internode<P>*>(n[sense]);
        in->prefetch();
        int kp = internode<P>::bound_type::upper(ka, *in);
        n[sense ^ 1] = in->child_[kp];
        if (!n[sense ^ 1]) {
            goto retry;
        }
        v[sense ^ 1] = n[sense ^ 1]->stable_annotated(ti.stable_fence());

        if (likely(!in->has_changed(v[sense]))) {
            sense ^= 1;
            continue;
        }

        typename node_base<P>::nodeversion_type oldv = v[sense];
        v[sense] = in->stable_annotated(ti.stable_fence());
        if (unlikely(oldv.has_split(v[sense]))
            && in->stable_last_key_compare(ka, v[sense], ti) > 0) {
            ti.mark(tc_root_retry);
            goto retry;
        } else {
            ti.mark(tc_internode_retry);
        }
    }

    version = v[sense];
    return const_cast<leaf<P> *>(static_cast<const leaf<P> *>(n[sense]));
}

/** @brief Return the leaf at or after *this responsible for @a ka.
    @pre *this was responsible for @a ka at version @a v

    Checks whether *this has split since version @a v. If it has split, then
    advances through the leaves using the B^link-tree pointers and returns
    the relevant leaf, setting @a v to the stable version for that leaf. */
template <typename P>
leaf<P>* leaf<P>::advance_to_key(const ikey_type& ka, nodeversion_type& v,
                                 threadinfo& ti) const
{
    const leaf<P>* n = this;
    nodeversion_type oldv = v;
    v = n->stable_annotated(ti.stable_fence());
    if (unlikely(v.has_split(oldv))
        && n->stable_last_key_compare(ka, v, ti) > 0) {
        leaf<P> *next;
        ti.mark(tc_leaf_walk);
        while (likely(!v.deleted())
               && (next = n->safe_next())
               && compare(ka.ikey(), next->ikey_bound()) >= 0) {
            n = next;
            v = n->stable_annotated(ti.stable_fence());
        }
    }
    return const_cast<leaf<P>*>(n);
}

template <typename P>
inline basic_table<P>::basic_table()
    : root_(0) {
}

template <typename P>
inline node_base<P>* basic_table<P>::root() const {
    return root_;
}

template <typename P>
inline node_base<P>* basic_table<P>::fix_root() {
    node_base<P>* root = root_;
    if (unlikely(!root->is_root())) {
        node_base<P>* old_root = root;
        root = root->maybe_parent();
        (void) cmpxchg(&root_, old_root, root);
    }
    return root;
}

}
#endif
