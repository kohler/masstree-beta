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
#ifndef VALUE_ARRAY_HH
#define VALUE_ARRAY_HH
#include "compiler.hh"
#include "kvrow.hh"

class value_array : public row_base<short> {
  public:
    typedef short index_type;
    static constexpr rowtype_id type_id = RowType_Array;
    static const char *name() { return "Array"; }

    inline value_array();

    inline kvtimestamp_t timestamp() const;
    inline int ncol() const;
    inline Str col(int i) const;

    void deallocate(threadinfo &ti);
    void deallocate_rcu(threadinfo &ti);

    template <typename CS>
    value_array* update(const CS& changeset, kvtimestamp_t ts, threadinfo& ti) const;
    template <typename CS>
    static value_array* create(const CS& changeset, kvtimestamp_t ts, threadinfo& ti);
    static inline value_array* create1(Str value, kvtimestamp_t ts, threadinfo& ti);
    template <typename CS>
    void deallocate_rcu_after_update(const CS& changeset, threadinfo& ti);
    template <typename CS>
    void deallocate_after_failed_update(const CS& changeset, threadinfo& ti);

    static value_array* checkpoint_read(Str str, kvtimestamp_t ts,
                                            threadinfo& ti);
    void checkpoint_write(kvout* buf) const;

    void print(FILE* f, const char* prefix, int indent, Str key,
	       kvtimestamp_t initial_ts, const char* suffix = "") {
	kvtimestamp_t adj_ts = timestamp_sub(ts_, initial_ts);
	fprintf(f, "%s%*s%.*s = ### @" PRIKVTSPARTS "%s\n", prefix, indent, "",
		key.len, key.s, KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    }

  private:
    kvtimestamp_t ts_;
    short ncol_;
    inline_string* cols_[0];

    static inline size_t shallow_size(int ncol);
    inline size_t shallow_size() const;
    static inline int count_columns(const change_t &c) {
	// Changes are sorted by field! Cheers!
	assert(c.size() && "Change can not be empty");
	return c[c.size() - 1].c_fid + 1;
    }
    void update(const change_t &c, threadinfo &ti);
    static value_array *make_sized_row(int ncol, kvtimestamp_t ts, threadinfo &ti);
};

inline value_array::value_array()
    : ts_(0), ncol_(0) {
}

inline kvtimestamp_t value_array::timestamp() const {
    return ts_;
}

inline int value_array::ncol() const {
    return ncol_;
}

inline Str value_array::col(int i) const {
    if (unsigned(i) < unsigned(ncol_))
        return Str(cols_[i]->s, cols_[i]->len);
    else
        return Str();
}

inline size_t value_array::shallow_size(int ncol) {
    return sizeof(value_array) + sizeof(inline_string*) * ncol;
}

inline size_t value_array::shallow_size() const {
    return shallow_size(ncol_);
}

template <typename CS>
value_array* value_array::update(const CS& changeset, kvtimestamp_t ts, threadinfo& ti) const {
    precondition(ts >= ts_);
    int ncol = std::max(int(ncol_), int(changeset.last_index()) + 1);
    value_array* row = (value_array*) ti.allocate(shallow_size(ncol), memtag_row_array);
    row->ts_ = ts;
    row->ncol_ = ncol;
    memcpy(row->cols_, cols_, ncol_ * sizeof(cols_[0]));
    memset(row->cols_ + ncol_, 0, (ncol - ncol_) * sizeof(cols_[0]));
    auto last = changeset.end();
    for (auto it = changeset.begin(); it != last; ++it)
        row->cols_[it->index()] = inline_string::allocate(it->value(), ti);
    return row;
}

template <typename CS>
value_array* value_array::create(const CS& changeset, kvtimestamp_t ts, threadinfo& ti) {
    value_array empty;
    return empty.update(changeset, ts, ti);
}

inline value_array* value_array::create1(Str value, kvtimestamp_t ts, threadinfo& ti) {
    value_array* row = (value_array*) ti.allocate(shallow_size(1), memtag_row_array);
    row->ts_ = ts;
    row->ncol_ = 1;
    row->cols_[0] = inline_string::allocate(value, ti);
    return row;
}

template <typename CS>
void value_array::deallocate_rcu_after_update(const CS& changeset, threadinfo& ti) {
    auto last = changeset.end();
    for (auto it = changeset.begin(); it != last && it->index() < ncol_; ++it)
        if (cols_[it->index()])
            cols_[it->index()]->deallocate_rcu(ti);
    ti.deallocate_rcu(this, shallow_size(), memtag_row_array);
}

template <typename CS>
void value_array::deallocate_after_failed_update(const CS& changeset, threadinfo& ti) {
    auto last = changeset.end();
    for (auto it = changeset.begin(); it != last; ++it)
        if (cols_[it->index()])
            cols_[it->index()]->deallocate(ti);
    ti.deallocate(this, shallow_size(), memtag_row_array);
}

#endif
