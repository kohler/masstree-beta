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
#ifndef STR_HH
#define STR_HH 1
#include "string_base.hh"
#include <stdarg.h>
#include <stdio.h>
struct kvin;

struct str : public String_base<str> {
    typedef str substring_type;

    const char *s;
    int len;

    str()
	: s(0), len(0) {
    }
    template <typename T>
    str(const String_base<T> &x)
	: s(x.data()), len(x.length()) {
    }
    str(const char *s_)
	: s(s_), len(strlen(s_)) {
    }
    str(const char *s_, int len_)
	: s(s_), len(len_) {
    }
    str(const char *sbegin, const char *send)
	: s(sbegin), len(send - sbegin) {
    }
    str(const uninitialized_type &unused) {
	(void) unused;
    }

    static const str maxkey;

    void assign() {
	s = 0;
	len = 0;
    }
    template <typename T>
    void assign(const String_base<T> &x) {
	s = x.data();
	len = x.length();
    }
    void assign(const char *s_) {
	s = s_;
	len = strlen(s_);
    }
    void assign(const char *s_, int len_) {
	s = s_;
	len = len_;
    }

    const char *data() const {
	return s;
    }
    int length() const {
	return len;
    }

    str substring(const char *first, const char *last) const {
	if (first <= last && first >= s && last <= s + len)
	    return str(first, last);
	else
	    return str();
    }
    str fast_substring(const char *first, const char *last) const {
	assert(begin() <= first && first <= last && last <= end());
	return str(first, last);
    }
    str ltrim() const {
	return String_generic::ltrim(*this);
    }
    str rtrim() const {
	return String_generic::rtrim(*this);
    }
    str trim() const {
	return String_generic::trim(*this);
    }

    long to_i() const {		// XXX does not handle negative
	long x = 0;
	int p;
	for (p = 0; p < len && s[p] >= '0' && s[p] <= '9'; ++p)
	    x = (x * 10) + s[p] - '0';
	return p == len && p != 0 ? x : -1;
    }

    static str snprintf(char *buf, size_t size, const char *fmt, ...) {
	va_list val;
	va_start(val, fmt);
	int n = vsnprintf(buf, size, fmt, val);
	va_end(val);
	return str(buf, n);
    }
};

struct inline_string : public String_base<inline_string> {
    int len;
    char s[0];

    const char *data() const {
	return s;
    }
    int length() const {
	return len;
    }

    size_t size() const {
	return sizeof(inline_string) + len;
    }
    static size_t size(int len) {
	return sizeof(inline_string) + len;
    }

    template <typename ALLOC>
    static inline_string *allocate(const char *s, int len, ALLOC &ti) {
	inline_string *r = (inline_string *) ti.allocate(size(len));
	r->len = len;
	memcpy(r->s, s, len);
	return r;
    }
    template <typename ALLOC>
    static inline_string *allocate(str s, ALLOC &ti) {
	return allocate(s.s, s.len, ti);
    }
    template <typename ALLOC>
    static inline_string *allocate_read(struct kvin *kvin, ALLOC &ti);
    template <typename ALLOC>
    void deallocate(ALLOC &ti) {
	ti.deallocate(this, size());
    }
    template <typename ALLOC>
    void deallocate_rcu(ALLOC &ti) {
	ti.deallocate_rcu(this, size());
    }
};

#endif
