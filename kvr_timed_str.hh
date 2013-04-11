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
#ifndef KVR_TIMED_STR_HH
#define KVR_TIMED_STR_HH
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

inline int KVR(kvin* kv, kvr_str_index::field_t& field) {
    int x = KVR(kv, field.f_off);
    return x + KVR(kv, field.f_len);
}

inline int KVW(kvout* kv, kvr_str_index::field_t field) {
    int x = KVW(kv, field.f_off);
    return x + KVW(kv, field.f_len);
}

struct kvr_timed_str : public row_base<kvr_str_index> {
    typedef kvr_str_index::field_t index_type;
    typedef kvr_str_index index_t;
    static constexpr bool has_priv_row_str = true;
    static constexpr rowtype_id type_id = RowType_Str;

    static const char *name() { return "String"; }

    inline kvr_timed_str();

    inline size_t size() const {
        return sizeof(kvr_timed_str) + vallen_;
    }
    inline int ncol() const {
        return 1;
    }
    inline Str col(int i) const {
	assert(i == 0);
	(void) i;
	return Str(s_, vallen_);
    }
    inline Str col(kvr_str_index::field_t idx) const {
        int len = idx.f_len == -1 ? vallen_ - idx.f_off : idx.f_len;
        return Str(s_ + idx.f_off, len);
    }

    inline void deallocate(threadinfo &ti) {
	ti.deallocate(this, size(), memtag_row_str);
    }
    inline void deallocate_rcu(threadinfo &ti) {
	ti.deallocate_rcu(this, size(), memtag_row_str);
    }

    template <typename CS>
    kvr_timed_str* update(const CS& changeset, kvtimestamp_t ts, threadinfo& ti) const;
    template <typename CS>
    static inline kvr_timed_str* create(const CS& changeset, kvtimestamp_t ts, threadinfo& ti);
    static inline kvr_timed_str* create1(Str value, kvtimestamp_t ts, threadinfo& ti);
    template <typename CS>
    inline void deallocate_rcu_after_update(const CS& changeset, threadinfo& ti);
    template <typename CS>
    inline void deallocate_after_failed_update(const CS& changeset, threadinfo& ti);

    /** @brief Update the row with change c.
     * @return a new kvr_timed_str if applied; NULL if change is out-of-dated
     */
    kvr_timed_str* update(const change_t& c, kvtimestamp_t ts, threadinfo& ti) const;
    /** @brief Convert a change to a timedvalue.
     */
    static kvr_timed_str* from_change(const change_t& c,
                                      kvtimestamp_t ts, threadinfo& ti);

    void print(FILE* f, const char* prefix, int indent, Str key,
	       kvtimestamp_t initial_ts, const char* suffix = "") {
	kvtimestamp_t adj_ts = timestamp_sub(ts_, initial_ts);
	fprintf(f, "%s%*s%.*s = %.*s @" PRIKVTSPARTS "%s\n", prefix, indent, "",
		key.len, key.s, std::min(40, vallen_), s_,
		KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    }

    static inline kvr_timed_str* checkpoint_read(Str str, kvtimestamp_t ts,
                                                 threadinfo& ti);
    inline void checkpoint_write(kvout* kv) const;

    kvtimestamp_t ts_;
  private:
    int vallen_;
    char s_[0];

    static inline size_t shallow_size(int vallen);
    inline size_t shallow_size() const;
};

inline kvr_timed_str::kvr_timed_str()
    : ts_(0), vallen_(0) {
}

inline size_t kvr_timed_str::shallow_size(int vallen) {
    return sizeof(kvr_timed_str) + vallen;
}

inline size_t kvr_timed_str::shallow_size() const {
    return shallow_size(vallen_);
}

template <typename CS>
kvr_timed_str* kvr_timed_str::update(const CS& changeset, kvtimestamp_t ts,
                                     threadinfo& ti) const {
    auto last = changeset.end();
    int vallen = 0, cut = vallen_;
    for (auto it = changeset.begin(); it != last; ++it) {
        vallen = std::max(vallen, it->index().f_off + it->value_length());
        if (it->index().f_len == -1)
            cut = std::min(cut, int(it->index().f_off));
    }
    vallen = std::max(vallen, cut);
    kvr_timed_str* row = (kvr_timed_str*) ti.allocate(shallow_size(vallen));
    row->ts_ = ts;
    row->vallen_ = vallen;
    memcpy(row->s_, s_, cut);
    for (auto it = changeset.begin(); it != last; ++it)
        memcpy(row->s_ + it->index().f_off, it->value().data(),
               it->value().length());
    return row;
}

template <typename CS>
inline kvr_timed_str* kvr_timed_str::create(const CS& changeset,
                                            kvtimestamp_t ts,
                                            threadinfo& ti) {
    kvr_timed_str empty;
    return empty.update(changeset, ts, ti);
}

inline kvr_timed_str* kvr_timed_str::create1(Str value,
                                             kvtimestamp_t ts,
                                             threadinfo& ti) {
    kvr_timed_str* row = (kvr_timed_str*) ti.allocate(shallow_size(value.length()));
    row->ts_ = ts;
    row->vallen_ = value.length();
    memcpy(row->s_, value.data(), value.length());
    return row;
}

template <typename CS>
inline void kvr_timed_str::deallocate_rcu_after_update(const CS&, threadinfo& ti) {
    deallocate_rcu(ti);
}

template <typename CS>
inline void kvr_timed_str::deallocate_after_failed_update(const CS&, threadinfo& ti) {
    deallocate(ti);
}

inline kvr_timed_str* kvr_timed_str::checkpoint_read(Str str,
                                                     kvtimestamp_t ts,
                                                     threadinfo& ti) {
    return create1(str, ts, ti);
}

inline void kvr_timed_str::checkpoint_write(kvout* kv) const {
    KVW(kv, Str(s_, vallen_));
}

#endif
