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
// -*- c-basic-offset: 4 -*-
#ifndef CLICK_PAIR_HH
#define CLICK_PAIR_HH
#include "hashcode.hh"

template <class T, class U>
struct Pair {

    typedef T first_type;
    typedef U second_type;
    typedef T key_type;
    typedef const T &key_const_reference;

    T first;
    U second;

    inline Pair()
	: first(), second() {
    }

    inline Pair(const T &t, const U &u)
	: first(t), second(u) {
    }

    inline Pair(const Pair<T, U> &p)
	: first(p.first), second(p.second) {
    }

    template <typename V, typename W>
    inline Pair(const Pair<V, W> &p)
	: first(p.first), second(p.second) {
    }

    typedef hashcode_t (Pair<T, U>::*unspecified_bool_type)() const;
    inline operator unspecified_bool_type() const {
	return first || second ? &Pair<T, U>::hashcode : 0;
    }

    inline const T &hashkey() const {
	return first;
    }

    inline hashcode_t hashcode() const;

    template <typename V, typename W>
    Pair<T, U> &operator=(const Pair<V, W> &p) {
	first = p.first;
	second = p.second;
	return *this;
    }

};

template <class T, class U>
inline bool operator==(const Pair<T, U> &a, const Pair<T, U> &b)
{
    return a.first == b.first && a.second == b.second;
}

template <class T, class U>
inline bool operator!=(const Pair<T, U> &a, const Pair<T, U> &b)
{
    return a.first != b.first || a.second != b.second;
}

template <class T, class U>
inline bool operator<(const Pair<T, U> &a, const Pair<T, U> &b)
{
    return a.first < b.first
	|| (!(b.first < a.first) && a.second < b.second);
}

template <class T, class U>
inline hashcode_t Pair<T, U>::hashcode() const
{
    return (hashcode(first) << 7) ^ hashcode(second);
}

template <class T, class U>
inline Pair<T, U> make_pair(T t, U u)
{
    return Pair<T, U>(t, u);
}

#endif
