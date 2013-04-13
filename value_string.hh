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

struct valueindex_string {
    short f_off;
    short f_len;
    valueindex_string() = default;
    valueindex_string(short off, short len)
        : f_off(off), f_len(len) {
    }
    friend bool operator==(const valueindex_string& a,
                           const valueindex_string& b) {
        return a.f_off == b.f_off && a.f_len == b.f_len;
    }
    friend bool operator<(const valueindex_string& a,
                          const valueindex_string& b) {
        return a.f_off < b.f_off;
    }
};

template <>
struct valueindex<valueindex_string> {
    static inline valueindex_string make_full() {
        return valueindex_string(0, -1);
    }
    static inline valueindex_string make_fixed(int index, int width) {
        return valueindex_string(index * width, width);
    }
};

inline int KVR(kvin* kv, valueindex_string& field) {
    int x = KVR(kv, field.f_off);
    return x + KVR(kv, field.f_len);
}

inline int KVW(kvout* kv, valueindex_string field) {
    int x = KVW(kv, field.f_off);
    return x + KVW(kv, field.f_len);
}

class value_string : public row_base<valueindex_string> {
  public:
    typedef valueindex_string index_type;
    static constexpr rowtype_id type_id = RowType_Str;
    static const char *name() { return "String"; }

    inline value_string();

    inline kvtimestamp_t timestamp() const;
    inline size_t size() const;
    inline int ncol() const;
    inline Str col(int i) const;
    inline Str col(valueindex_string idx) const;

    template <typename ALLOC>
    inline void deallocate(ALLOC& ti);
    inline void deallocate_rcu(threadinfo& ti);

    template <typename CS, typename ALLOC>
    value_string* update(const CS& changeset, kvtimestamp_t ts, ALLOC& ti) const;
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

  private:
    kvtimestamp_t ts_;
    int vallen_;
    char s_[0];

    static inline size_t shallow_size(int vallen);
    inline size_t shallow_size() const;
};

inline value_string::value_string()
    : ts_(0), vallen_(0) {
}

inline kvtimestamp_t value_string::timestamp() const {
    return ts_;
}

inline size_t value_string::size() const {
    return sizeof(value_string) + vallen_;
}

inline int value_string::ncol() const {
    return 1;
}

inline Str value_string::col(int i) const {
    assert(i == 0);
    (void) i;
    return Str(s_, vallen_);
}

inline Str value_string::col(valueindex_string idx) const {
    int len = idx.f_len == -1 ? vallen_ - idx.f_off : idx.f_len;
    return Str(s_ + idx.f_off, len);
}

template <typename ALLOC>
inline void value_string::deallocate(ALLOC& ti) {
    ti.deallocate(this, size(), memtag_row_str);
}

inline void value_string::deallocate_rcu(threadinfo& ti) {
    ti.deallocate_rcu(this, size(), memtag_row_str);
}

inline size_t value_string::shallow_size(int vallen) {
    return sizeof(value_string) + vallen;
}

inline size_t value_string::shallow_size() const {
    return shallow_size(vallen_);
}

template <typename CS, typename ALLOC>
value_string* value_string::update(const CS& changeset, kvtimestamp_t ts,
                                   ALLOC& ti) const {
    auto last = changeset.end();
    int vallen = 0, cut = vallen_;
    for (auto it = changeset.begin(); it != last; ++it) {
        vallen = std::max(vallen, it->index().f_off + it->value_length());
        if (it->index().f_len == -1)
            cut = std::min(cut, int(it->index().f_off));
    }
    vallen = std::max(vallen, cut);
    value_string* row = (value_string*) ti.allocate(shallow_size(vallen), memtag_row_str);
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
    value_string* row = (value_string*) ti.allocate(shallow_size(value.length()), memtag_row_str);
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
