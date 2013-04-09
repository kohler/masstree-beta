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
#ifndef VEC_HH
#define VEC_HH
#include <unistd.h>
#include <assert.h>

namespace KUtil {
template <typename T>
class vec {
  public:
    vec(size_t size) {
        init(size);
    }
    vec() {
        init(0);
    }
    vec(const vec<T> &v) {
        init(0);
        assign(v);
    }
    size_t size() const {
        return size_;
    }
    ~vec() {
        if (a_)
            delete[] a_;
    }
    vec<T> &operator=(const vec<T> &v) {
         return assign(v);
    }
    vec<T> &assign(const vec<T> &v) {
        if (&v == this)
            return *this;
        if (a_)
            delete[] a_;
        for (size_t i = 0; i < v.size_; i++)
            push_back(v[i]);
        return *this;
    }
    T &operator[](unsigned i) {
        precondition(i < size_);
        return a_[i];
    }
    const T &operator[](unsigned i) const {
        precondition(i < size_);
        return a_[i];
    }
    T &push_back(const T &t) {
        resize(size_ + 1);
        a_[size_ - 1] = t;
        return a_[size_ - 1];
    }
    void clear() {
        resize(0);
    }
    void resize(size_t size) {
        if (size > alen_) {
            T *na = new T[size];
            for (size_t i = 0; i < size_; i++)
                na[i] = a_[i];
            if (a_)
                delete[] a_;
            a_ = na;
            alen_ = size;
        }
        size_ = size;
    }

    typedef T *iterator;
    typedef const T *const_iterator;
    iterator begin() {
	return a_;
    }
    iterator end() {
	return a_ + size_;
    }
    const_iterator begin() const {
	return a_;
    }
    const_iterator end() const {
	return a_ + size_;
    }
    const_iterator cbegin() const {
	return a_;
    }
    const_iterator cend() const {
	return a_ + size_;
    }
  private:
    void init(size_t size) {
        a_ = size ? (new T[size]) : NULL;
        alen_ = size_ = size;
    }
    T *a_;
    size_t size_;
    size_t alen_;
};
}

#endif
