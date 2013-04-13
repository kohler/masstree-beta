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
#ifndef VALUE_VERSIONED_ARRAY_HH
#define VALUE_VERSIONED_ARRAY_HH
#include "compiler.hh"
#include "value_array.hh"

struct rowversion {
    rowversion() {
	v_.u = 0;
    }
    bool dirty() {
        return v_.dirty;
    }
    void setdirty() {
        v_.u = v_.u | 0x80000000;
    }
    void clear() {
        v_.u = v_.u & 0x7fffffff;
    }
    void clearandbump() {
        v_.u = (v_.u + 1) & 0x7fffffff;
    }
    rowversion stable() const {
        value_t x = v_;
        while (x.dirty) {
            relax_fence();
            x = v_;
        }
        acquire_fence();
        return x;
    }
    bool has_changed(rowversion x) const {
        fence();
        return x.v_.ctr != v_.ctr;
    }
  private:
    union value_t {
        struct {
            uint32_t ctr:31;
            uint32_t dirty:1;
        };
        uint32_t u;
    };
    value_t v_;

    rowversion(value_t v)
        : v_(v) {
    }

};

class value_versioned_array : public row_base<value_array::index_type> {
  public:
    typedef value_array::index_type index_type;
    static constexpr rowtype_id type_id = RowType_ArrayVer;
    static const char *name() { return "ArrayVersion"; }

    inline value_versioned_array();

    inline kvtimestamp_t timestamp() const;
    inline int ncol() const;
    inline Str col(int i) const;

    void deallocate(threadinfo &ti);
    void deallocate_rcu(threadinfo &ti);

    void snapshot(value_versioned_array*& storage,
                  const fields_t& f, threadinfo& ti) const;

    template <typename CS>
    value_versioned_array* update(const CS& changeset, kvtimestamp_t ts, threadinfo& ti, bool always_copy = false);
    template <typename CS>
    static value_versioned_array* create(const CS& changeset, kvtimestamp_t ts, threadinfo& ti);
    static value_versioned_array* create1(Str value, kvtimestamp_t ts, threadinfo& ti);
    template <typename CS>
    inline void deallocate_rcu_after_update(const CS& changeset, threadinfo& ti);
    template <typename CS>
    inline void deallocate_after_failed_update(const CS& changeset, threadinfo& ti);

    static value_versioned_array* checkpoint_read(Str str, kvtimestamp_t ts,
                                                threadinfo& ti);
    void checkpoint_write(kvout* kv) const;

    void print(FILE *f, const char *prefix, int indent, Str key,
	       kvtimestamp_t initial_ts, const char *suffix = "") {
	kvtimestamp_t adj_ts = timestamp_sub(ts_, initial_ts);
	fprintf(f, "%s%*s%.*s = ### @" PRIKVTSPARTS "%s\n", prefix, indent, "",
		key.len, key.s, KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    }

  private:
    kvtimestamp_t ts_;
    rowversion ver_;
    short ncol_;
    short ncol_cap_;
    inline_string* cols_[0];

    static inline size_t shallow_size(int ncol);
    inline size_t shallow_size() const;
    static value_versioned_array* make_sized_row(int ncol, kvtimestamp_t ts, threadinfo& ti);
};

template <>
struct query_helper<value_versioned_array> {
    value_versioned_array* snapshot_;

    query_helper()
        : snapshot_() {
    }
    inline const value_versioned_array* snapshot(const value_versioned_array* row, const value_versioned_array::fields_t& f, threadinfo& ti) {
        row->snapshot(snapshot_, f, ti);
        return snapshot_;
    }
};

inline value_versioned_array::value_versioned_array()
    : ts_(0), ncol_(0), ncol_cap_(0) {
}

inline kvtimestamp_t value_versioned_array::timestamp() const {
    return ts_;
}

inline int value_versioned_array::ncol() const {
    return ncol_;
}

inline Str value_versioned_array::col(int i) const {
    if (unsigned(i) < unsigned(ncol_))
        return Str(cols_[i]->s, cols_[i]->len);
    else
        return Str();
}

inline size_t value_versioned_array::shallow_size(int ncol) {
    return sizeof(value_versioned_array) + ncol * sizeof(inline_string*);
}

inline size_t value_versioned_array::shallow_size() const {
    return shallow_size(ncol_);
}

template <typename CS>
value_versioned_array* value_versioned_array::update(const CS& changeset, kvtimestamp_t ts, threadinfo& ti, bool always_copy) {
    int ncol = changeset.last_index() + 1;
    value_versioned_array* row;
    if (ncol > ncol_cap_ || always_copy) {
        row = (value_versioned_array*) ti.allocate(shallow_size(ncol), memtag_row_array_ver);
        row->ts_ = ts;
        row->ver_ = rowversion();
        row->ncol_ = row->ncol_cap_ = ncol;
        memcpy(row->cols_, cols_, sizeof(cols_[0]) * ncol_);
    } else
        row = this;
    if (ncol > ncol_)
        memset(row->cols_ + ncol_, 0, sizeof(cols_[0]) * (ncol - ncol_));

    if (row == this) {
        ver_.setdirty();
        fence();
    }
    if (row->ncol_ < ncol)
        row->ncol_ = ncol;

    auto last = changeset.end();
    for (auto it = changeset.begin(); it != last; ++it) {
        if (row->cols_[it->index()])
            row->cols_[it->index()]->deallocate_rcu(ti);
        row->cols_[it->index()] = inline_string::allocate(it->value(), ti);
    }

    if (row == this) {
        fence();
        ver_.clearandbump();
    }
    return row;
}

template <typename CS>
value_versioned_array* value_versioned_array::create(const CS& changeset, kvtimestamp_t ts, threadinfo& ti) {
    value_versioned_array empty;
    return empty.update(changeset, ts, ti, true);
}

inline value_versioned_array* value_versioned_array::create1(Str value, kvtimestamp_t ts, threadinfo& ti) {
    value_versioned_array* row = (value_versioned_array*) ti.allocate(shallow_size(1), memtag_row_array_ver);
    row->ts_ = ts;
    row->ver_ = rowversion();
    row->ncol_ = row->ncol_cap_ = 1;
    row->cols_[0] = inline_string::allocate(value, ti);
    return row;
}

template <typename CS>
inline void value_versioned_array::deallocate_rcu_after_update(const CS&, threadinfo& ti) {
    ti.deallocate_rcu(this, shallow_size(), memtag_row_array_ver);
}

template <typename CS>
inline void value_versioned_array::deallocate_after_failed_update(const CS&, threadinfo&) {
    mandatory_assert(0);
}

#endif
