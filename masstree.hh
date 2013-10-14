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
#ifndef MASSTREE_HH
#define MASSTREE_HH
#include "compiler.hh"
#include "str.hh"
#include "ksearch.hh"

namespace Masstree {
using lcdf::Str;
using lcdf::String;

template <typename T> class value_print;

template <int LW = 15, int IW = LW> struct nodeparams {
    static constexpr int leaf_width = LW;
    static constexpr int internode_width = IW;
    static constexpr bool concurrent = true;
    static constexpr bool prefetch = true;
    static constexpr int bound_method = bound_method_binary;
    static constexpr int debug_level = 0;
    static constexpr bool printable_keys = true;
    typedef uint64_t ikey_type;
};

template <int LW, int IW> constexpr int nodeparams<LW, IW>::leaf_width;
template <int LW, int IW> constexpr int nodeparams<LW, IW>::internode_width;
template <int LW, int IW> constexpr int nodeparams<LW, IW>::debug_level;

template <typename P> class node_base;
template <typename P> class leaf;
template <typename P> class internode;
template <typename P> class leafvalue;
template <typename P> class key;
template <typename P> class basic_table;
template <typename P> class unlocked_tcursor;
template <typename P> class tcursor;

template <typename P>
class basic_table {
  public:
    typedef P param_type;
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef typename P::value_type value_type;
    typedef typename P::threadinfo_type threadinfo;
    typedef unlocked_tcursor<P> unlocked_cursor_type;
    typedef tcursor<P> cursor_type;

    inline basic_table();

    void initialize(threadinfo& ti);
    void destroy(threadinfo& ti);

    inline node_type* root() const;
    inline node_type* fix_root();

    bool get(Str key, value_type& value, threadinfo& ti) const;

    template <typename F>
    int scan(Str firstkey, bool matchfirst, F& scanner, threadinfo& ti) const;
    template <typename F>
    int rscan(Str firstkey, bool matchfirst, F& scanner, threadinfo& ti) const;

    template <typename F>
    inline int modify(Str key, F& f, threadinfo& ti);
    template <typename F>
    inline int modify_insert(Str key, F& f, threadinfo& ti);

    inline void print(FILE* f = 0, int indent = 0) const;

  private:
    node_type* root_;

    template <typename H, typename F>
    int scan(H helper, Str firstkey, bool matchfirst,
	     F& scanner, threadinfo& ti) const;

    friend class unlocked_tcursor<P>;
    friend class tcursor<P>;
};

} // namespace Masstree
#endif
