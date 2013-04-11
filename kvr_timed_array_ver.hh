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
#ifndef KVR_TIMED_ARRAY_VER_HH
#define KVR_TIMED_ARRAY_VER_HH
#include "compiler.hh"
#include "kvr_timed_array.hh"

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

struct kvr_timed_array_ver : public row_base<kvr_array_index> {
    typedef kvr_timed_array::index_type index_type;
    typedef kvr_array_index index_t;
    static constexpr bool has_priv_row_str = false;
    static constexpr rowtype_id type_id = RowType_ArrayVer;

    static const char *name() { return "ArrayVersion"; }

    inline kvr_timed_array_ver();

    inline int ncol() const {
        return ncol_;
    }
    inline Str col(int i) const {
	if (unsigned(i) < unsigned(ncol_))
	    return Str(cols_[i]->s, cols_[i]->len);
	else
	    return Str();
    }

    void snapshot(kvr_timed_array_ver*& storage,
                  const fields_t& f, threadinfo& ti) const;

    void deallocate(threadinfo &ti);
    void deallocate_rcu(threadinfo &ti);

    template <typename CS>
    kvr_timed_array_ver* update(const CS& changeset, kvtimestamp_t ts, threadinfo& ti, bool always_copy = false);
    template <typename CS>
    static kvr_timed_array_ver* create(const CS& changeset, kvtimestamp_t ts, threadinfo& ti);
    static kvr_timed_array_ver* create1(Str value, kvtimestamp_t ts, threadinfo& ti);
    template <typename CS>
    inline void deallocate_rcu_after_update(const CS& changeset, threadinfo& ti);
    template <typename CS>
    inline void deallocate_after_failed_update(const CS& changeset, threadinfo& ti);

    static kvr_timed_array_ver* checkpoint_read(Str str, kvtimestamp_t ts,
                                                threadinfo& ti);
    void checkpoint_write(kvout* kv) const;

    void print(FILE *f, const char *prefix, int indent, Str key,
	       kvtimestamp_t initial_ts, const char *suffix = "") {
	kvtimestamp_t adj_ts = timestamp_sub(ts_, initial_ts);
	fprintf(f, "%s%*s%.*s = ### @" PRIKVTSPARTS "%s\n", prefix, indent, "",
		key.len, key.s, KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    }

    kvtimestamp_t ts_;
  private:
    rowversion ver_;
    short ncol_;
    short ncol_cap_;
    inline_string* cols_[0];

    static inline size_t shallow_size(int ncol);
    inline size_t shallow_size() const;
    static kvr_timed_array_ver* make_sized_row(int ncol, kvtimestamp_t ts, threadinfo& ti);
};

template <>
struct query_helper<kvr_timed_array_ver> {
    kvr_timed_array_ver* snapshot_;

    query_helper()
        : snapshot_() {
    }
    inline const kvr_timed_array_ver* snapshot(const kvr_timed_array_ver* row, const kvr_timed_array_ver::fields_t& f, threadinfo& ti) {
        row->snapshot(snapshot_, f, ti);
        return snapshot_;
    }
};

inline kvr_timed_array_ver::kvr_timed_array_ver()
    : ts_(0), ncol_(0), ncol_cap_(0) {
}

inline size_t kvr_timed_array_ver::shallow_size(int ncol) {
    return sizeof(kvr_timed_array_ver) + ncol * sizeof(inline_string*);
}

inline size_t kvr_timed_array_ver::shallow_size() const {
    return shallow_size(ncol_);
}

template <typename CS>
kvr_timed_array_ver* kvr_timed_array_ver::update(const CS& changeset, kvtimestamp_t ts, threadinfo& ti, bool always_copy) {
    int ncol = changeset.last_index() + 1;
    kvr_timed_array_ver* row;
    if (ncol > ncol_cap_ || always_copy) {
        row = (kvr_timed_array_ver*) ti.allocate(shallow_size(ncol), memtag_row_array_ver);
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
kvr_timed_array_ver* kvr_timed_array_ver::create(const CS& changeset, kvtimestamp_t ts, threadinfo& ti) {
    kvr_timed_array_ver empty;
    return empty.update(changeset, ts, ti, true);
}

inline kvr_timed_array_ver* kvr_timed_array_ver::create1(Str value, kvtimestamp_t ts, threadinfo& ti) {
    kvr_timed_array_ver* row = (kvr_timed_array_ver*) ti.allocate(shallow_size(1), memtag_row_array_ver);
    row->ts_ = ts;
    row->ver_ = rowversion();
    row->ncol_ = row->ncol_cap_ = 1;
    row->cols_[0] = inline_string::allocate(value, ti);
    return row;
}

template <typename CS>
inline void kvr_timed_array_ver::deallocate_rcu_after_update(const CS&, threadinfo& ti) {
    ti.deallocate_rcu(this, shallow_size(), memtag_row_array_ver);
}

template <typename CS>
inline void kvr_timed_array_ver::deallocate_after_failed_update(const CS&, threadinfo&) {
    mandatory_assert(0);
}

#endif
