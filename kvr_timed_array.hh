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
#ifndef KVR_TIMED_ARRAY_HH
#define KVR_TIMED_ARRAY_HH 1
#include "compiler.hh"
#include "kvrow.hh"

struct kvr_array_index {
    typedef short field_t;
    static int kvread_field(struct kvin *kvin, field_t &f) {
        KVR(kvin, f);
        return sizeof(f);
    }
    static int kvwrite_field(struct kvout *kvout, const field_t &f) {
        KVW(kvout, f);
        return sizeof(f);
    }
    static void make_full_field(field_t &f) {
        f = 0;
    }
    static void make_fixed_width_field(field_t &f, int idx, int) {
        f = idx;
    }
    static field_t make_fixed_width_field(int idx, int) {
        return idx;
    }
};

struct kvr_timed_array : public row_base<kvr_array_index> {
    typedef struct kvr_array_index index_t;
    static constexpr bool has_priv_row_str = false;
    static constexpr rowtype_id type_id = RowType_Array;

    static const char *name() { return "Array"; }

    str col(int i) const {
	if (unsigned(i) < unsigned(ncol_))
	    return str(cols_[i]->s, cols_[i]->len);
	else
	    return str();
    }

    void deallocate(threadinfo &ti);
    void deallocate_rcu(threadinfo &ti);
    void deallocate_rcu_after_update(const change_t &c, threadinfo &ti);
    void deallocate_after_failed_update(const change_t &c, threadinfo &ti);

    /** @brief Update the row with change c.
     * @return a new kvr_timed_array if applied; NULL if change is out-of-dated
     */
    kvr_timed_array *update(const change_t &c, kvtimestamp_t ts, threadinfo &ti) const;
    /** @brief Convert a change to a timedvalue without keyrest.
     */
    static kvr_timed_array *from_change(const change_t &c,
                                        kvtimestamp_t ts, threadinfo &ti);
    void filteremit(const fields_t &f, query<kvr_timed_array> &q,
		    struct kvout *kvout) const;
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
    static kvr_timed_array *from_rowstr(str, kvtimestamp_t, threadinfo &);
    kvtimestamp_t ts_;
  private:
    static int kvwrite_column(struct kvout *kvout, inline_string *c);
    short ncol_;
    inline_string *cols_[0];
    inline size_t shallow_size() const {
        return sizeof(kvr_timed_array) + ncol_ * sizeof(cols_[0]);
    }
    static inline int count_columns(const change_t &c) {
	// Changes are sorted by field! Cheers!
	assert(c.size() && "Change can not be empty");
	return c[c.size() - 1].c_fid + 1;
    }
    void update(const change_t &c, threadinfo &ti);
    static kvr_timed_array *make_sized_row(int ncol, kvtimestamp_t ts, threadinfo &ti);
};

#endif
