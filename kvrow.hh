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
#ifndef KVROW_HH
#define KVROW_HH 1
#include "vec.hh"
#include "kvio.hh"
#include "misc.hh"
#include "kvthread.hh"
#include "kvtable.hh"
#include "log.hh"
#include <algorithm>

template <typename IDX>
struct row_base {
    struct cell_t {
        typename IDX::field_t c_fid;
        str c_value;
	friend bool operator<(const cell_t &a, const cell_t &b) {
	    return a.c_fid < b.c_fid;
	}
    };
    typedef KUtil::vec<cell_t> change_t;
    typedef KUtil::vec<typename IDX::field_t> fields_t;
    static int parse_change(str v, change_t &c) {
        struct kvin kvin;
        kvin_init(&kvin, const_cast<char *>(v.s), v.len);
        return kvread_change(&kvin, c);
    }
    static int parse_fields(str v, fields_t &f) {
        struct kvin kvin;
        kvin_init(&kvin, const_cast<char *>(v.s), v.len);
        return kvread_fields(&kvin, f);
    }
    static int kvread_fields(struct kvin *kvin, fields_t &f) {
        short n;
        KVR(kvin, n);
        f.resize(n);
        for (short i = 0; i < n; i++)
            IDX::kvread_field(kvin, f[i]);
        return 0;
    }
    static int kvwrite_fields(struct kvout *kvout, const fields_t &f) {
        short n = f.size();
	for (short i = 1; i < n; i++)
	    if (!(f[i - 1] < f[i])) {
	        assert(0 && "The fields must be sorted");
		exit(EXIT_FAILURE);
	    }
        KVW(kvout, n);
        for (short i = 0; i < n; i++)
            IDX::kvwrite_field(kvout, f[i]);
        return 0;
    }
    static int kvread_change(struct kvin *kvin, change_t &c) {
        short n;
        KVR(kvin, n);
        c.resize(n);
        for (short i = 0; i < n; i++) {
            IDX::kvread_field(kvin, c[i].c_fid);
            kvread_str_inplace(kvin, c[i].c_value);
        }
        return 0;
    }
    static void sort(change_t &c) {
	std::sort(c.begin(), c.end());
    }
    static void sort(fields_t &f) {
	std::sort(f.begin(), f.end());
    }
    static int kvwrite_change(struct kvout *kvout, const change_t &c) {
        short n = c.size();
	for (short i = 1; i < n; i++)
	    if (!(c[i - 1] < c[i])) {
	        assert(0 && "The change must be sorted");
		exit(EXIT_FAILURE);
	    }
        KVW(kvout, n);
        for (short i = 0; i < n; i++) {
            IDX::kvwrite_field(kvout, c[i].c_fid);
            kvwrite_str(kvout, c[i].c_value);
        }
        return 0;
    }

    static cell_t make_cell(typename IDX::field_t fid, str value) {
	cell_t c;
	c.c_fid = fid;
	c.c_value = value;
	return c;
    }

    /** @brief Interfaces for column-less key/value store. */
    static void make_get1_fields(fields_t &f) {
        f.resize(1);
        IDX::make_full_field(f[0]);
    }
    static void make_put1_change(change_t &c, str val) {
        c.resize(1);
        IDX::make_full_field(c[0].c_fid);
        c[0].c_value = val;
    }
    static str make_put_col_request(struct kvout *kvout,
				    typename IDX::field_t fid,
				    str value) {
	kvout_reset(kvout);
	KVW(kvout, short(1));
	IDX::kvwrite_field(kvout, fid);
	kvwrite_str(kvout, value);
	return str(kvout->buf, kvout->n);
    }
};


struct row_marker {
    enum { mt_remove = 1, mt_delta = 2 };
    int marker_type_;
};

template <typename R>
struct row_delta_marker : public row_marker {
    kvtimestamp_t prev_ts_;
    R *prev_;
    char s_[0];
};

template <typename R>
inline bool
row_is_marker(const R *row)
{
    return row->ts_ & 1;
}

template <typename R>
inline bool
row_is_delta_marker(const R *row)
{
    if (row_is_marker(row)) {
	const row_marker *m =
	    reinterpret_cast<const row_marker *>(row->col(0).s);
	return m->marker_type_ == m->mt_delta;
    } else
	return false;
}

template <typename R>
inline row_delta_marker<R> *
row_get_delta_marker(const R *row, bool force = false)
{
    (void) force;
    assert(force || row_is_delta_marker(row));
    return reinterpret_cast<row_delta_marker<R> *>
	(const_cast<char *>(row->col(0).s));
}


template <typename R>
struct query_helper {
};
template <>
struct query_helper<kvr_timed_array_ver> {
    KUtil::vec<void *> snapshot;
};

template <typename R>
struct query {
    enum {
	QT_None = 0,
	QT_Get = 1,
	QT_Scan = 2,
	QT_Ckp_Scan = 3,
	QT_Get1_Col0 = 4, /* + column index */

	QT_Put = 4,
	QT_Remove = 5,
	QT_MinReplay = 7,
	QT_Ckp_Put = 7,
	QT_Replay_Put = 8,
	QT_Replay_Remove = 9,
	QT_Replay_Modify = 10
    };

    void begin_get(str key, str req, struct kvout *kvout);
    void begin_put(str key, str req);
    void begin_replay_put(str key, str req, kvtimestamp_t ts);
    void begin_replay_modify(str key, str req, kvtimestamp_t ts,
			     kvtimestamp_t prev_ts);
    void begin_scan(str startkey, int npairs, str req,
		    struct kvout *kvout);
    void begin_checkpoint(ckstate *ck, str startkey, str endkey);
    void begin_remove(str key);
    void begin_replay_remove(str key, kvtimestamp_t ts, threadinfo *ti);
    /** @brief Insert the string representation of a row from checkpoint.
     */
    void begin_ckp_put(str key, str ckv, kvtimestamp_t ts);

    /** @brief interfaces where the value is a single column,
     *    and where "get" does not emit but save a copy locally.
     */
    void begin_get1(str key, int col = 0) {
	qt_ = QT_Get1_Col0 + col;
	key_ = key;
    }
    str get1_value() const {
	return val_;
    }
    void begin_scan1(str startkey, int npairs, struct kvout *kvout);
    void begin_put1(str key, str value);
    void begin_replay_put1(str key, str value, kvtimestamp_t ts);

    int query_type() const {
	return qt_;
    }
    kvtimestamp_t query_timestamp() const {
	return ts_;
    }
    kvtimestamp_t query_prev_timestamp() const {
	return prev_ts_;
    }
    kvepoch_t query_epoch() const {
	return epoch_;
    }

    inline bool emitrow(const R *v);
    /** @return whether the scan should continue or not
     */
    bool scanemit(str k, const R *v);

    inline result_t apply_put(R *&value, bool has_value, threadinfo *ti);
    inline bool apply_remove(R *&value, bool has_value, threadinfo *ti, kvtimestamp_t *node_ts = 0);
  private:
    typename R::change_t c_;
    typename R::fields_t f_;
    unsigned long scan_npairs_;
    kvtimestamp_t ts_;
    kvtimestamp_t prev_ts_;
    kvepoch_t epoch_;
  public:
    str key_;   // startkey for scan; key for others
    kvout *kvout_;
    query_helper<R> helper_;
  private:
    int qt_;    // query type
    ckstate *ck_;
    str endkey_;
    str val_;			// value for Get1 and CkpPut
    void assign_timestamp(threadinfo *ti);
    void assign_timestamp(threadinfo *ti, kvtimestamp_t t);
    result_t apply_replay(R *&value, bool has_value, threadinfo *ti);
};

template <typename R>
void
query<R>::begin_get(str key, str req, struct kvout *kvout)
{
    qt_ = QT_Get;
    key_ = key;
    R::parse_fields(req, f_);
    kvout_ = kvout;
}

template <typename R>
void
query<R>::begin_put(str key, str req)
{
    qt_ = QT_Put;
    key_ = key;
    R::parse_change(req, c_);
}

template <typename R>
void
query<R>::begin_replay_put(str key, str req, kvtimestamp_t ts)
{
    qt_ = QT_Replay_Put;
    key_ = key;
    R::parse_change(req, c_);
    ts_ = ts;
}

template <typename R>
void
query<R>::begin_put1(str key, str val)
{
    qt_ = QT_Put;
    key_ = key;
    R::make_put1_change(c_, val);
}

template <typename R>
void
query<R>::begin_replay_put1(str key, str value, kvtimestamp_t ts)
{
    qt_ = QT_Replay_Put;
    key_ = key;
    R::make_put1_change(c_, value);
    ts_ = ts;
}

template <typename R>
void
query<R>::begin_replay_modify(str key, str req,
			      kvtimestamp_t ts, kvtimestamp_t prev_ts)
{
    // XXX We assume that sizeof(row_delta_marker<R>) memory exists before
    // 'req's string data. We don't modify this memory but it must be
    // readable. This is OK for conventional log replay, but that's an ugly
    // interface
    qt_ = QT_Replay_Modify;
    key_ = key;
    R::parse_change(req, c_);
    val_ = req;
    ts_ = ts;
    prev_ts_ = prev_ts;
}

template <typename R>
void
query<R>::begin_scan(str startkey, int npairs, str req,
		     struct kvout *kvout)
{
    assert(npairs > 0);
    qt_ = QT_Scan;
    key_ = startkey;
    R::parse_fields(req, f_);
    scan_npairs_ = npairs;
    kvout_ = kvout;
}

template <typename R>
void
query<R>::begin_scan1(str startkey, int npairs, struct kvout *kvout)
{
    assert(npairs > 0);
    qt_ = QT_Scan;
    key_ = startkey;
    R::make_get1_fields(f_);
    scan_npairs_ = npairs;
    kvout_ = kvout;
}

template <typename R>
void
query<R>::begin_checkpoint(ckstate *ck, str startkey, str endkey)
{
    qt_ = QT_Ckp_Scan;
    key_ = startkey;
    ck_ = ck;
    endkey_ = endkey;
}

template <typename R>
void
query<R>::begin_remove(str key)
{
    qt_ = QT_Remove;
    key_ = key;
}

template <typename R>
void
query<R>::begin_replay_remove(str key, kvtimestamp_t ts, threadinfo *ti)
{
    qt_ = QT_Replay_Remove;
    key_ = key;
    ts_ = ts | 1;		// marker timestamp
    row_marker *m = reinterpret_cast<row_marker *>(ti->buf_);
    m->marker_type_ = row_marker::mt_remove;
    R::make_put1_change(c_, str(ti->buf_, sizeof(*m)));
}

template <typename R>
void
query<R>::begin_ckp_put(str key, str val, kvtimestamp_t ts)
{
    qt_ = QT_Ckp_Put;
    key_ = key;
    val_ = val;
    ts_ = ts;
}

template <typename R>
bool
query<R>::scanemit(str k, const R *v)
{
    if (row_is_marker(v))
	return true;
    if (qt_ == QT_Ckp_Scan) {
        if (endkey_ && k >= endkey_)
            return false;
        ::checkpoint1(ck_, k, v);
        return true;
    } else {
        assert(qt_ == QT_Scan);
	kvwrite_str(kvout_, k);
        v->filteremit(f_, *this, kvout_);
        --scan_npairs_;
        return scan_npairs_ > 0;
    }
}

template <typename R>
inline bool
query<R>::emitrow(const R *v)
{
    if (row_is_marker(v))
	return false;
    else if (qt_ >= QT_Get1_Col0) {
	val_ = v->col(qt_ - QT_Get1_Col0);
	return true;
    } else {
        assert(qt_ == QT_Get);
        v->filteremit(f_, *this, kvout_);
	return true;
    }
}

template <typename R>
inline void
query<R>::assign_timestamp(threadinfo *ti)
{
    if (qt_ < QT_MinReplay) {
	prev_ts_ = 0;
	ts_ = ti->update_timestamp();
    }
}

template <typename R>
inline void
query<R>::assign_timestamp(threadinfo *ti, kvtimestamp_t min_ts)
{
    if (qt_ < QT_MinReplay) {
	prev_ts_ = min_ts;
	ts_ = ti->update_timestamp(min_ts);
    }
}

template <typename R>
result_t
query<R>::apply_replay(R *&value, bool has_value, threadinfo *ti)
{
    assert(qt_ != QT_Ckp_Put || !has_value);

    R **cur_value = &value;
    if (!has_value)
	*cur_value = 0;

    // find point to insert change (may be after some delta markers)
    while (*cur_value && row_is_delta_marker(*cur_value)
	   && (*cur_value)->ts_ > ts_)
	cur_value = &row_get_delta_marker(*cur_value)->prev_;

    // check out of date
    if (*cur_value && (*cur_value)->ts_ >= ts_)
	return OutOfDate;

    // if not modifying, delete everything earlier
    if (qt_ != QT_Replay_Modify)
	while (R *old_value = *cur_value) {
	    if (row_is_delta_marker(old_value)) {
		ti->pstat.mark_delta_removed();
		*cur_value = row_get_delta_marker(old_value)->prev_;
	    } else
		*cur_value = 0;
	    old_value->deallocate(*ti);
	}

    // actually apply change
    if (qt_ == QT_Ckp_Put)
        *cur_value = R::from_rowstr(val_, ts_, *ti);
    else if (qt_ != QT_Replay_Modify)
	*cur_value = R::from_change(c_, ts_, *ti);
    else {
	if (*cur_value && (*cur_value)->ts_ == prev_ts_) {
	    R *old_value = *cur_value;
	    *cur_value = old_value->update(c_, ts_, *ti);
	    if (*cur_value != old_value)
		old_value->deallocate(*ti);
	} else {
	    // XXX assume that memory exists before saved request -- it does
	    // in conventional log replay, but that's an ugly interface
	    val_.s -= sizeof(row_delta_marker<R>);
	    val_.len += sizeof(row_delta_marker<R>);
	    R::make_put1_change(c_, val_);
	    R *new_value = R::from_change(c_, ts_ | 1, *ti);
	    row_delta_marker<R> *dm = row_get_delta_marker(new_value, true);
	    dm->marker_type_ = row_marker::mt_delta;
	    dm->prev_ts_ = prev_ts_;
	    dm->prev_ = *cur_value;
	    *cur_value = new_value;
	    ti->pstat.mark_delta_created();
	}
    }

    // clean up
    while (value && row_is_delta_marker(value)) {
	R **prev = 0, **trav = &value;
	while (*trav && row_is_delta_marker(*trav)) {
	    prev = trav;
	    trav = &row_get_delta_marker(*trav)->prev_;
	}
	if (prev && *trav
	    && row_get_delta_marker(*prev)->prev_ts_ == (*trav)->ts_) {
	    R *old_prev = *prev;
	    str req = old_prev->col(0);
	    req.s += sizeof(row_delta_marker<R>);
	    req.len -= sizeof(row_delta_marker<R>);
	    R::parse_change(req, c_);
	    *prev = (*trav)->update(c_, old_prev->ts_ - 1, *ti);
	    if (*prev != *trav)
		(*trav)->deallocate(*ti);
	    old_prev->deallocate(*ti);
	    ti->pstat.mark_delta_removed();
	} else
	    break;
    }

    return Updated;
}

template <typename R>
inline result_t
query<R>::apply_put(R *&value, bool has_value, threadinfo *ti)
{
    if (qt_ >= QT_MinReplay)
	return apply_replay(value, has_value, ti);

    if (struct log *log = ti->ti_log) {
	log->acquire();
	epoch_ = global_log_epoch;
    }

    if (!has_value) {
    insert:
	assign_timestamp(ti);
        value = R::from_change(c_, ts_, *ti);
	return Inserted;
    }

    R *old_value = value;
    assign_timestamp(ti, old_value->ts_);
    if (row_is_marker(old_value)) {
	old_value->deallocate_rcu(*ti);
	goto insert;
    }

    R *updated = old_value->update(c_, ts_, *ti);
    if (updated != old_value) {
	value = updated;
	old_value->deallocate_rcu_after_update(c_, *ti);
    }
    return Updated;
}

template <typename R>
inline bool query<R>::apply_remove(R *&value, bool has_value, threadinfo *ti,
				   kvtimestamp_t *node_ts)
{
    if (!has_value)
	return false;

    if (struct log *log = ti->ti_log) {
	log->acquire();
	epoch_ = global_log_epoch;
    }

    R *old_value = value;
    assign_timestamp(ti, old_value->ts_);
    if (node_ts && circular_int<kvtimestamp_t>::less_equal(*node_ts, ts_))
	*node_ts = ts_ + 2;
    old_value->deallocate_rcu(*ti);
    return true;
}


template <typename R>
struct query_scanner {
    query<R> &q_;
    query_scanner(query<R> &q)
	: q_(q) {
    }
    bool operator()(str key, R *value, threadinfo *) {
	return q_.scanemit(key, value);
    }
};

#include KVDB_ROW_TYPE_INCLUDE
#endif
