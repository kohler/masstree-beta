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
#ifndef MASSTREE_HH
#define MASSTREE_HH 1
#include "compiler.hh"
#include "kvtable.hh"
#include "nodeversion.hh"
#include "ksearch.hh"

namespace Masstree {

template <int LW, int IW = LW> struct nodeparams {
    static constexpr int leaf_width = LW;
    static constexpr int internode_width = IW;
    static constexpr bool concurrent = true;
    static constexpr bool prefetch = true;
    static constexpr int bound_method = bound_method_binary;
    static constexpr int debug_level = 0;
    typedef uint64_t ikey_type;
    typedef row_type *value_type;
};

template <int LW, int IW> constexpr int nodeparams<LW, IW>::leaf_width;
template <int LW, int IW> constexpr int nodeparams<LW, IW>::internode_width;
template <int LW, int IW> constexpr int nodeparams<LW, IW>::debug_level;

template <typename P> struct node_base;
template <typename P> struct leaf;
template <typename P> struct internode;
template <typename P> struct leafvalue;
template <typename P> struct key;
template <typename P> class basic_table;
template <typename P> class unlocked_tcursor;
template <typename P> class tcursor;

template <typename P>
class simple_table {
  public:
    typedef P param_type;
    typedef node_base<P> node_type;
    typedef typename P::value_type value_type;

    simple_table()
        : root_(0) {
    }

    void initialize(threadinfo *ti);
    void reinitialize(threadinfo *ti);

    bool get(const str &key, value_type &value,
	     threadinfo *ti) const;

    template <typename F>
    int scan(const str &firstkey, bool matchfirst, F &scanner,
	     threadinfo *ti) const;
    template <typename F>
    int rscan(const str &firstkey, bool matchfirst, F &scanner,
	      threadinfo *ti) const;

    template <typename F>
    inline int modify(const str &key, F &f,
		      threadinfo *ti);
    template <typename F>
    inline int modify_insert(const str &key, F &f,
			     threadinfo *ti);

  private:
    node_type *root_;

    template <typename H, typename F>
    int scan(H helper, const str &firstkey, bool matchfirst,
	     F &scanner, threadinfo *ti) const;

    friend class basic_table<P>;
    friend class unlocked_tcursor<P>;
    friend class tcursor<P>;
};

template <typename P>
class basic_table {
  public:

    typedef P param_type;
    typedef node_base<P> node_type;

    basic_table() {
    }

    simple_table<P> &table() {
	return table_;
    }

    void initialize(threadinfo *ti) {
        table_.initialize(ti);
    }
    void reinitialize(threadinfo *ti) {
        table_.reinitialize(ti);
    }

    bool get(query<row_type> &q, threadinfo *ti) const;
    void scan(query<row_type> &q, threadinfo *ti) const;
    void rscan(query<row_type> &q, threadinfo *ti) const;

    result_t put(query<row_type> &q, threadinfo *ti);
    bool remove(query<row_type> &q, threadinfo *ti);

    void findpivots(str *pv, int npv) const;
    ckptrav_order_t ckptravorder() const {
        return ckptrav_inorder;
    }

    void stats(FILE *f);
    void json_stats(Json &j, threadinfo *ti);

    void print(FILE *f, int indent) const;

    static void test(threadinfo *ti);

    static const char *name() {
	return "mb";
    }

  private:
    simple_table<P> table_;

};

typedef basic_table<nodeparams<15, 15> > default_table;

} // namespace Masstree

template <typename P> struct table_has_print<Masstree::basic_table<P> > : public mass::true_type {};
template <typename P> struct table_has_remove<Masstree::basic_table<P> > : public mass::true_type {};
template <typename P> struct table_has_rscan<Masstree::basic_table<P> > : public mass::true_type {};
template <typename P> struct table_has_json_stats<Masstree::basic_table<P> > : public mass::true_type {};

#endif
