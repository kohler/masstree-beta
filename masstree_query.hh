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
#ifndef MASSTREE_QUERY_HH
#define MASSTREE_QUERY_HH 1
#include "masstree.hh"
#include "kvtable.hh"

namespace Masstree {

template <typename P>
class query_table {
  public:
    typedef P param_type;
    typedef node_base<P> node_type;

    query_table() {
    }

    basic_table<P> &table() {
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
    basic_table<P> table_;

};

struct default_query_table_params : public nodeparams<15, 15> {
    typedef row_type *value_type;
};

typedef query_table<default_query_table_params> default_table;

} // namespace Masstree

template <typename P> struct table_has_print<Masstree::query_table<P> > : public mass::true_type {};
template <typename P> struct table_has_remove<Masstree::query_table<P> > : public mass::true_type {};
template <typename P> struct table_has_rscan<Masstree::query_table<P> > : public mass::true_type {};
template <typename P> struct table_has_json_stats<Masstree::query_table<P> > : public mass::true_type {};

#endif
