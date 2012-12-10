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
#ifndef KVR_TIMED_ARRAY_VER_HH
#define KVR_TIMED_ARRAY_VER_HH 1
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
    typedef struct kvr_array_index index_t;
    static constexpr bool has_priv_row_str = false;
    static constexpr rowtype_id type_id = RowType_ArrayVer;

    static const char *name() { return "ArrayVersion"; }

    inline str col(int i) const {
	if (unsigned(i) < unsigned(ncol_))
	    return str(cols_[i]->s, cols_[i]->len);
	else
	    return str();
    }

    void deallocate(threadinfo &ti);
    void deallocate_rcu(threadinfo &ti);
    void deallocate_rcu_after_update(const change_t &, threadinfo &ti) {
	ti.deallocate_rcu(this, shallow_size(), memtag_row_array_ver);
    }
    void deallocate_after_failed_update(const change_t &, threadinfo &) {
	assert(0);
    }

    /** @brief Update the row with change c.
     * @return a new kvr_timed_array_ver if applied; NULL if change is out-of-dated
     */
    kvr_timed_array_ver *update(const change_t &c, kvtimestamp_t ts, threadinfo &ti);
    /** @brief Convert a change to a timedvalue
     */
    static kvr_timed_array_ver *from_change(const change_t &c,
                                            kvtimestamp_t ts, threadinfo &ti);
    void filteremit(const fields_t &f, query<kvr_timed_array_ver> &q, struct kvout *kvout) const;
    void print(FILE *f, const char *prefix, int indent, str key,
	       kvtimestamp_t initial_ts, const char *suffix = "") {
	kvtimestamp_t adj_ts = timestamp_sub(ts_, initial_ts);
	fprintf(f, "%s%*s%.*s = ### @" PRIKVTSPARTS "%s\n", prefix, indent, "",
		key.len, key.s, KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    }

    void to_priv_row_str(str &) const {
        assert(0 && "To private string is not available");
    }
    /** @brief Return the string representation of the value of the row
     *    (excluding the timestamp).
     *    The string is stored in @a buffer.
     *    Should not use this version for timed_str.
     */
    void to_shared_row_str(str &val, kvout *buffer) const;
    static kvr_timed_array_ver *from_rowstr(str, kvtimestamp_t, threadinfo &);
    kvtimestamp_t ts_;
  private:
    rowversion ver_;
    short ncol_;
    inline_string *cols_[0];
    inline size_t shallow_size() const {
        return sizeof(kvr_timed_array_ver) + ncol_ * sizeof(cols_[0]);
    }
    static inline int count_columns(const change_t &c) {
	// Changes are sorted by field! Cheers!
	assert(c.size() && "Change can not be empty");
	return c[c.size() - 1].c_fid + 1;
    }
    void update(const change_t &c, threadinfo &ti);
    static kvr_timed_array_ver *make_sized_row(int ncol, kvtimestamp_t ts, threadinfo &ti);
};

#endif
