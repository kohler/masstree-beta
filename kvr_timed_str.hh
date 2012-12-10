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
#ifndef KVR_TIMED_STR_HH
#define KVR_TIMED_STR_HH 1
#include "compiler.hh"
#include "kvrow.hh"

struct kvr_str_index {
    struct field_t {
        short f_off;
        short f_len;
	friend bool operator<(const field_t &a, const field_t &b) {
	    return a.f_off < b.f_off;
	}
    };
    static int kvread_field(struct kvin *kvin, field_t &f) {
        KVR(kvin, f.f_off);
        KVR(kvin, f.f_len);
        return sizeof(struct field_t);
    }
    static int kvwrite_field(struct kvout *kvout, const field_t &f) {
        KVW(kvout, f.f_off);
        KVW(kvout, f.f_len);
        return sizeof(struct field_t);
    }
    static void make_full_field(field_t &f) {
        f.f_off = 0;
        f.f_len = -1;
    }
    static void make_fixed_width_field(field_t &f, int idx, int width) {
        f.f_off = idx * width;
        f.f_len = width;
    }
    static field_t make_fixed_width_field(int idx, int width) {
        field_t f;
        f.f_off = idx * width;
        f.f_len = width;
        return f;
    }
};

struct kvr_timed_str : public row_base<kvr_str_index> {
    typedef struct kvr_str_index index_t;
    static constexpr bool has_priv_row_str = true;
    static constexpr rowtype_id type_id = RowType_Str;

    static const char *name() { return "String"; }

    inline size_t size() const {
        return sizeof(kvr_timed_str) + vallen_;
    }
    inline str col(int i) const {
	assert(i == 0);
	(void) i;
	return str(s_, vallen_);
    }

    inline void deallocate(threadinfo &ti) {
	ti.deallocate(this, size(), memtag_row_str);
    }
    inline void deallocate_rcu(threadinfo &ti) {
	ti.deallocate_rcu(this, size(), memtag_row_str);
    }
    inline void deallocate_rcu_after_update(const change_t &, threadinfo &ti) {
	deallocate_rcu(ti);
    }
    inline void deallocate_after_failed_update(const change_t &, threadinfo &ti) {
	deallocate(ti);
    }

    /** @brief Update the row with change c.
     * @return a new kvr_timed_str if applied; NULL if change is out-of-dated
     */
    kvr_timed_str *update(const change_t &c, kvtimestamp_t ts, threadinfo &ti) const;
    /** @brief Convert a change to a timedvalue.
     */
    static kvr_timed_str *from_change(const change_t &c,
                                      kvtimestamp_t ts, threadinfo &ti);
    void filteremit(const fields_t &f, query<kvr_timed_str> &q,
		    struct kvout *kvout) const;
    void print(FILE *f, const char *prefix, int indent, str key,
	       kvtimestamp_t initial_ts, const char *suffix = "") {
	kvtimestamp_t adj_ts = timestamp_sub(ts_, initial_ts);
	fprintf(f, "%s%*s%.*s = %.*s @" PRIKVTSPARTS "%s\n", prefix, indent, "",
		key.len, key.s, std::min(40, vallen_), s_,
		KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    }
    void to_priv_row_str(str &val) const {
        val.assign(s_, vallen_);
    }
    void to_shared_row_str(str &, kvout *) const {
        assert(0 && "Use to_priv_rowstr for performance!");
    }
    /** @brief Return a row object from a string that is created by to_privstr
     *    or to_sharedstr.
     */
    static kvr_timed_str *from_rowstr(str, kvtimestamp_t, threadinfo &);
    kvtimestamp_t ts_;
  private:
    int vallen_;
    char s_[0];
    static int endat(const change_t &, bool &toend);
    void update(const change_t &c);
    static kvr_timed_str *make_sized_row(int vlen, kvtimestamp_t ts, threadinfo &ti);
};

#endif
