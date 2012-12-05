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
#ifndef KVR_TIMED_BAG_HH
#define KVR_TIMED_BAG_HH 1
#include "kvthread.hh"
#include "kvrow.hh"

struct kvr_bag_index {
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

template <typename O>
struct kvr_timed_bag : public row_base<kvr_bag_index> {
    typedef struct kvr_bag_index index_t;
    typedef O offset_type;

  private:
    union bagdata {
	struct {
	    offset_type ncol_;
	    offset_type pos_[1];
	};
	char s_[0];
    };

  public:
    static constexpr bool has_priv_row_str = true;
    static constexpr rowtype_id type_id = RowType_Bag;

    kvr_timed_bag()
	: ts_(0) {
	d_.ncol_ = 0;
	d_.pos_[0] = sizeof(bagdata);
    }

    static const char *name() { return "Bag"; }

    inline size_t size() const {
        return sizeof(kvtimestamp_t) + d_.pos_[d_.ncol_];
    }
    inline int ncol() const {
	return d_.ncol_;
    }
    inline str col(int i) const {
	if (unsigned(i) < unsigned(d_.ncol_))
	    return str(d_.s_ + d_.pos_[i], d_.pos_[i + 1] - d_.pos_[i]);
	else
	    return str();
    }

    template <typename ALLOC>
    inline void deallocate(ALLOC &ti) {
	ti.deallocate(this, size());
    }
    template <typename ALLOC>
    inline void deallocate_rcu(ALLOC &ti) {
	ti.deallocate_rcu(this, size());
    }
    template <typename ALLOC>
    inline void deallocate_rcu_after_update(const change_t &, ALLOC &ti) {
	deallocate_rcu(ti);
    }
    template <typename ALLOC>
    inline void deallocate_after_failed_update(const change_t &, ALLOC &ti) {
	deallocate(ti);
    }

    /** @brief Update the row with change c.
     * @return a new kvr_timed_bag if applied; NULL if change is out-of-dated
     */
    template <typename ALLOC>
    kvr_timed_bag<O> *update(const change_t &c, kvtimestamp_t ts,
			     ALLOC &ti) const;
    template <typename ALLOC>
    inline kvr_timed_bag<O> *update(int col, str value,
				    kvtimestamp_t ts, ALLOC &ti) const {
	change_t c;
	c.push_back(this->make_cell(col, value));
	return update(c, ts, ti);
    }
    /** @brief Convert a change to a timedvalue.
     */
    template <typename ALLOC>
    static kvr_timed_bag<O> *from_change(const change_t &c,
					 kvtimestamp_t ts, ALLOC &ti) {
	kvr_timed_bag<O> empty_bag;
	return empty_bag.update(c, ts, ti);
    }
    void filteremit(const fields_t &f, query<kvr_timed_bag<O> > &q,
		    struct kvout *kvout) const;
    void print(FILE *f, const char *prefix, int indent, str key,
	       kvtimestamp_t initial_ts, const char *suffix = "");
    void to_priv_row_str(str &val) const {
        val.assign(d_.s_, d_.pos_[d_.ncol_]);
    }
    str row_string() const {
	return str(d_.s_, d_.pos_[d_.ncol_]);
    }
    void to_shared_row_str(str &, kvout *) const {
        assert(0 && "Use to_priv_rowstr for performance!");
    }
    /** @brief Return a row object from a string that is created by to_privstr
     *    or to_sharedstr.
     */
    template <typename ALLOC>
    static kvr_timed_bag<O> *from_rowstr(str, kvtimestamp_t,
					 ALLOC &);
    kvtimestamp_t ts_;
  private:
    bagdata d_;
};


template <typename O> template <typename ALLOC>
kvr_timed_bag<O> *kvr_timed_bag<O>::update(const change_t &c, kvtimestamp_t ts,
					   ALLOC &ti) const
{
    size_t sz = size();
    change_t::const_iterator cb = c.begin(), ce = c.end();
    for (change_t::const_iterator it = cb; it != ce; ++it) {
	sz += it->c_value.len;
	if (it->c_fid < d_.ncol_)
	    sz -= (d_.pos_[it->c_fid + 1] - d_.pos_[it->c_fid]);
    }
    int ncol = ce[-1].c_fid + 1;
    if (ncol > d_.ncol_)
	sz += (ncol - d_.ncol_) * sizeof(offset_type);
    else
	ncol = d_.ncol_;

    kvr_timed_bag<O> *row = (kvr_timed_bag<O> *) ti.allocate(sz);
    row->ts_ = ts;

    // Minor optimization: Replacing one small column without changing length
    if (ncol == d_.ncol_ && sz == size() && c.size() == 1
	&& cb->c_value.len <= 16) {
	memcpy(row->d_.s_, d_.s_, sz - sizeof(kvtimestamp_t));
	memcpy(row->d_.s_ + d_.pos_[cb->c_fid],
	       cb->c_value.s, cb->c_value.len);
	return row;
    }

    // Otherwise need to do more work
    row->d_.ncol_ = ncol;
    sz = sizeof(bagdata) + ncol * sizeof(offset_type);
    int col = 0;
    while (1) {
	int this_col = (cb == ce ? ncol : cb->c_fid);

	// copy data from old row
	if (col != this_col && col < d_.ncol_) {
	    int end_col = std::min(this_col, int(d_.ncol_));
	    ssize_t delta = sz - d_.pos_[col];
	    if (delta == 0)
		memcpy(row->d_.pos_ + col, d_.pos_ + col,
		       sizeof(offset_type) * (end_col - col));
	    else
		for (int i = col; i < end_col; ++i)
		    row->d_.pos_[i] = d_.pos_[i] + delta;
	    size_t amt = d_.pos_[end_col] - d_.pos_[col];
	    memcpy(row->d_.s_ + sz, d_.s_ + sz - delta, amt);
	    col = end_col;
	    sz += amt;
	}
	while (col != this_col) {
	    row->d_.pos_[col] = sz;
	    ++col;
	}

	if (col == ncol)
	    break;

	// copy data from change
	row->d_.pos_[col] = sz;
	memcpy(row->d_.s_ + sz, cb->c_value.s, cb->c_value.len);
	sz += cb->c_value.len;
	++cb;
	++col;
    }
    row->d_.pos_[ncol] = sz;
    return row;
}

template <typename O>
void kvr_timed_bag<O>::filteremit(const fields_t &f, query<kvr_timed_bag<O> > &, struct kvout *kvout) const
{
    short n = f.size();
    if (n == 0) {
        KVW(kvout, (short)1);
        kvwrite_str(kvout, str(d_.s_, d_.pos_[d_.ncol_]));
    } else {
        KVW(kvout, n);
        for (int i = 0; i < n; i++)
	    kvwrite_str(kvout, str(d_.s_ + d_.pos_[f[i]], d_.pos_[f[i]+1] - d_.pos_[f[i]]));
    }
}

template <typename O> template <typename ALLOC>
kvr_timed_bag<O> *kvr_timed_bag<O>::from_rowstr(str rb, kvtimestamp_t ts,
						ALLOC &ti)
{
    kvr_timed_bag<O> *row = (kvr_timed_bag<O> *) ti.allocate(sizeof(kvtimestamp_t) + rb.len);
    row->ts_ = ts;
    memcpy(row->d_.s_, rb.s, rb.len);
    return row;
}

template <typename O>
void kvr_timed_bag<O>::print(FILE *f, const char *prefix, int indent,
			     str key, kvtimestamp_t initial_ts,
			     const char *suffix)
{
    kvtimestamp_t adj_ts = timestamp_sub(ts_, initial_ts);
    if (d_.ncol_ == 1)
	fprintf(f, "%s%*s%.*s = %.*s @" PRIKVTSPARTS "%s\n", prefix, indent, "",
		key.len, key.s, d_.pos_[1] - d_.pos_[0], d_.s_ + d_.pos_[0],
		KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    else {
	fprintf(f, "%s%*s%.*s = [", prefix, indent, "", key.len, key.s);
	for (int col = 0; col < d_.ncol_; ++col) {
	    int pos = d_.pos_[col], len = std::min(40, d_.pos_[col + 1] - pos);
	    fprintf(f, col ? "|%.*s" : "%.*s", len, d_.s_ + pos);
	}
	fprintf(f, "] @" PRIKVTSPARTS "%s\n",
		KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    }
}

#endif
