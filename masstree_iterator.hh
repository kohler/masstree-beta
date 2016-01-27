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
#ifndef MASSTREE_ITERATOR_HH
#define MASSTREE_ITERATOR_HH
#include "masstree_scan.hh"

namespace Masstree {
template <typename P>
class basic_table<P>::iterator
    : std::iterator<std::forward_iterator_tag, itvalue_type> {
    typedef typename P::ikey_type ikey_type;
    typedef typename node_type::key_type key_type;
    typedef typename node_type::leaf_type::leafvalue_type leafvalue_type;
    typedef typename node_type::nodeversion_type nodeversion_type;
    typedef typename leaf_type::permuter_type permuter_type;
    typedef typename leaf_type::bound_type bound_type;

  public:
    iterator(basic_table<P>* table, threadinfo* ti, Str firstkey = "");
    static iterator make_end(basic_table<P>* table, threadinfo *ti);

    itvalue_type& operator*() { assert(!end_); return pair_; };
    itvalue_type* operator->() { assert(!end_); return &pair_; };
    bool operator==(const iterator& rhs) { return (end_ == rhs.end_) && (end_ || ka_.compare(rhs.ka_) == 0); };
    bool operator!=(const iterator& rhs) { return !(*this == rhs); };
    iterator operator++() { advance(); return *this; };
    iterator operator++(int) { iterator it = *this; advance(); return it; };

  private:
    basic_table<P>* table_;
    threadinfo* ti_;
    key_type ka_;
    value_type value_;
    itvalue_type pair_;
    bool end_;
    union {
        ikey_type x[(MASSTREE_MAXKEYLEN + sizeof(ikey_type) - 1)/sizeof(ikey_type)];
        char s[MASSTREE_MAXKEYLEN];
    } keybuf_;

    void advance(bool emit_equal = false);


    // Debugging support.
    int id_;
    static int count_;

    void dprintf(const char *format, ...) {
        va_list args;
        va_start(args, format);
        fprintf(stderr, "it%d: ", id_);
        vfprintf(stderr, format, args);
        va_end(args);
    }
};

template <typename P> int basic_table<P>::iterator::count_ = 0;

template <typename P>
basic_table<P>::iterator::iterator(basic_table<P>* table, threadinfo* ti, Str firstkey)
    : table_(table), ti_(ti), end_(false), id_(count_++) {
    masstree_precondition(firstkey.len <= (int) sizeof(keybuf_));
    memcpy(keybuf_.s, firstkey.s, firstkey.len);
    ka_ = key_type(keybuf_.s, firstkey.len);

    advance(true);
};

template <typename P>
typename basic_table<P>::iterator
basic_table<P>::iterator::make_end(basic_table<P>* table, threadinfo *ti) {
    iterator it = iterator(table, ti);
    it.end_ = true;
    return it;
}

template <typename P>
void
basic_table<P>::iterator::advance(bool emit_equal) {
    key_indexed_position kip;
    leaf_type* n;
    nodeversion_type v;
    node_type* root;
    permuter_type perm;
    Str suffix;
    char suffixbuf[MASSTREE_MAXKEYLEN];

 retry_root:
    ka_.unshift_all();
    root = table_->root();
    n = root->reach_leaf(ka_, v, *ti_);
    perm = n->permutation();
    kip = bound_type::lower(ka_, *n);

 retry:
    if (v.deleted())
        goto retry_root;

    if (unsigned(kip.i) >= unsigned(perm.size())) {
        n = n->safe_next();
        if (!n) {
            if (root == table_->root()) {
                end_ = true;
                return;
            }

            ka_.unshift();
            while (ka_.increment() && ka_.is_shifted())
                ka_.unshift();
            ka_.assign_store_ikey(ka_.ikey());
            ka_.assign_store_length(ka_.ikey_size);
            goto retry_root;
        }
        perm = n->permutation();
        v = n->stable();
        kip = bound_type::lower(ka_, *n);
        goto retry;
    }

    int kp = perm[kip.i];
    int keylenx = n->keylenx_[kp];
    ikey_type ikey = n->ikey0_[kp];
    leafvalue_type entry = n->lv_[kp];
    if (n->keylenx_has_ksuf(keylenx)) {
        suffix = n->ksuf(kp);
        memcpy(suffixbuf, suffix.s, suffix.len);
        suffix.s = suffixbuf;
    }

    if (n->has_changed(v))
        goto retry_root;

    ka_.assign_store_ikey(ikey);
    if (n->keylenx_is_layer(keylenx)) {
        usleep(1);
        ka_.shift();
        root = entry.layer();
        n = root->reach_leaf(ka_, v, *ti_);
        perm = n->permutation();
        kip = bound_type::lower(ka_, *n);
        goto retry;
    }

    if (!emit_equal && kip.p >= 0 &&
        (!n->keylenx_has_ksuf(keylenx) || suffix.compare(ka_.suffix()) == 0)) {
        kip.i++;
        kip.p = -1;
        goto retry;
    }

    int keylen = keylenx;
    if (n->keylenx_has_ksuf(keylenx)) {
        keylen = ka_.assign_store_suffix(suffix);
    }
    ka_.assign_store_length(keylen);
    ka_.unshift_all();
    pair_ = itvalue_type(ka_, entry.value());
}

template <typename P>
typename basic_table<P>::iterator
basic_table<P>::begin(threadinfo& ti) {
    return iterator(this, &ti);
}

template <typename P>
typename basic_table<P>::iterator
basic_table<P>::end(threadinfo& ti) {
    return iterator::make_end(this, &ti);
}

template <typename P>
typename basic_table<P>::iterator
basic_table<P>::iterate_from(Str firstkey, threadinfo& ti) {
    return iterator(this, &ti, firstkey);
}
}
#endif
