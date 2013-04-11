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
#ifndef VALUE_STRING_HH
#define VALUE_STRING_HH
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

class value_string : public row_base<kvr_str_index> {
  public:
    typedef kvr_str_index::field_t index_type;
    typedef kvr_str_index index_t;
    static constexpr rowtype_id type_id = RowType_Str;

    static const char *name() { return "String"; }

    inline value_string();

    inline size_t size() const {
        return sizeof(value_string) + vallen_;
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
    value_string* update(const CS& changeset, kvtimestamp_t ts, threadinfo& ti) const;
    template <typename CS>
    static inline value_string* create(const CS& changeset, kvtimestamp_t ts, threadinfo& ti);
    static inline value_string* create1(Str value, kvtimestamp_t ts, threadinfo& ti);
    template <typename CS>
    inline void deallocate_rcu_after_update(const CS& changeset, threadinfo& ti);
    template <typename CS>
    inline void deallocate_after_failed_update(const CS& changeset, threadinfo& ti);

    static inline value_string* checkpoint_read(Str str, kvtimestamp_t ts,
                                                 threadinfo& ti);
    inline void checkpoint_write(kvout* kv) const;

    void print(FILE* f, const char* prefix, int indent, Str key,
	       kvtimestamp_t initial_ts, const char* suffix = "") {
	kvtimestamp_t adj_ts = timestamp_sub(ts_, initial_ts);
	fprintf(f, "%s%*s%.*s = %.*s @" PRIKVTSPARTS "%s\n", prefix, indent, "",
		key.len, key.s, std::min(40, vallen_), s_,
		KVTS_HIGHPART(adj_ts), KVTS_LOWPART(adj_ts), suffix);
    }

    kvtimestamp_t ts_;
  private:
    int vallen_;
    char s_[0];

    static inline size_t shallow_size(int vallen);
    inline size_t shallow_size() const;
};

inline value_string::value_string()
    : ts_(0), vallen_(0) {
}

inline size_t value_string::shallow_size(int vallen) {
    return sizeof(value_string) + vallen;
}

inline size_t value_string::shallow_size() const {
    return shallow_size(vallen_);
}

template <typename CS>
value_string* value_string::update(const CS& changeset, kvtimestamp_t ts,
                                   threadinfo& ti) const {
    auto last = changeset.end();
    int vallen = 0, cut = vallen_;
    for (auto it = changeset.begin(); it != last; ++it) {
        vallen = std::max(vallen, it->index().f_off + it->value_length());
        if (it->index().f_len == -1)
            cut = std::min(cut, int(it->index().f_off));
    }
    vallen = std::max(vallen, cut);
    value_string* row = (value_string*) ti.allocate(shallow_size(vallen));
    row->ts_ = ts;
    row->vallen_ = vallen;
    memcpy(row->s_, s_, cut);
    for (auto it = changeset.begin(); it != last; ++it)
        memcpy(row->s_ + it->index().f_off, it->value().data(),
               it->value().length());
    return row;
}

template <typename CS>
inline value_string* value_string::create(const CS& changeset,
                                          kvtimestamp_t ts,
                                          threadinfo& ti) {
    value_string empty;
    return empty.update(changeset, ts, ti);
}

inline value_string* value_string::create1(Str value,
                                           kvtimestamp_t ts,
                                           threadinfo& ti) {
    value_string* row = (value_string*) ti.allocate(shallow_size(value.length()));
    row->ts_ = ts;
    row->vallen_ = value.length();
    memcpy(row->s_, value.data(), value.length());
    return row;
}

template <typename CS>
inline void value_string::deallocate_rcu_after_update(const CS&, threadinfo& ti) {
    deallocate_rcu(ti);
}

template <typename CS>
inline void value_string::deallocate_after_failed_update(const CS&, threadinfo& ti) {
    deallocate(ti);
}

inline value_string* value_string::checkpoint_read(Str str,
                                                   kvtimestamp_t ts,
                                                   threadinfo& ti) {
    return create1(str, ts, ti);
}

inline void value_string::checkpoint_write(kvout* kv) const {
    KVW(kv, Str(s_, vallen_));
}

#endif
