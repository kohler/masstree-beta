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
#ifndef OLFIT_HH
#define OLFIT_HH
#include "../compiler.hh"
#include "../str.hh"
#include "../ksearch.hh"

namespace OLFIT {
using lcdf::Str;
using lcdf::String;

class key_unparse_printable_string;
template <typename T> class value_print;

template <int F = 15> struct nodeparams {
    static constexpr int fanout = 15;
    static constexpr bool concurrent = true;
    static constexpr bool prefetch = true;
    static constexpr int bound_method = bound_method_binary;
    static constexpr int debug_level = 0;
    static constexpr int ikey_size = 16;
    typedef uint32_t nodeversion_value_type;
    static constexpr bool need_phantom_epoch = true;
    typedef uint64_t phantom_epoch_type;
    static constexpr ssize_t print_max_indent_depth = 12;
    typedef key_unparse_printable_string key_unparse_type;
};

template <int F> constexpr int nodeparams<F>::fanout;
template <int F> constexpr int nodeparams<F>::debug_level;

template <typename P> class node_base;
template <typename P> class leaf;
template <typename P> class internode;
template <size_t S> class fix_sized_key;
template <typename P> class basic_table;
template <typename P> class unlocked_tcursor;
template <typename P> class tcursor;

template <typename P>
class basic_table {
  public:
    using parameter_type = P;
    using node_type = node_base<P>;
    using leaf_type = leaf<P>;
    using value_type = typename P::value_type;
    using threadinfo = typename P::threadinfo_type;
    using unlocked_cursor_type = unlocked_tcursor<P>;
    using cursor_type = tcursor<P>;

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

    inline void print(FILE* f = 0) const;

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
