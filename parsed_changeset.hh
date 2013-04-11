#ifndef MASSTREE_PARSED_CHANGESET_HH
#define MASSTREE_PARSED_CHANGESET_HH
#include <vector>

template <typename IDX>
class parsed_change {
  public:
    inline parsed_change() = default;
    inline parsed_change(IDX index, Str value);

    inline IDX index() const;
    inline Str value() const;
    inline int32_t value_length() const;

  private:
    IDX index_;
    Str value_;
};

template <typename IDX>
class parsed_changeset {
  public:
    inline parsed_changeset() = default;

    inline void push_back(const parsed_change<IDX>& x);
    inline void emplace_back(IDX index, Str value);

    inline bool empty() const;
    inline bool single_index() const;
    inline IDX last_index() const;

    typedef typename std::vector<parsed_change<IDX> >::const_iterator iterator;
    inline iterator begin() const;
    inline iterator end() const;

  private:
    std::vector<parsed_change<IDX> > v_;
};


template <typename IDX>
inline parsed_change<IDX>::parsed_change(IDX index, Str value)
    : index_(index), value_(value) {
}

template <typename IDX>
inline IDX parsed_change<IDX>::index() const {
    return index_;
}

template <typename IDX>
inline int32_t parsed_change<IDX>::value_length() const {
    return value_.length();
}

template <typename IDX>
inline Str parsed_change<IDX>::value() const {
    return value_;
}

template <typename IDX>
inline void parsed_changeset<IDX>::push_back(const parsed_change<IDX>& x) {
    v_.push_back(x);
}

template <typename IDX>
inline void parsed_changeset<IDX>::emplace_back(IDX index, Str value) {
    v_.emplace_back(index, value);
}

template <typename IDX>
inline bool parsed_changeset<IDX>::empty() const {
    return v_.empty();
}

template <typename IDX>
inline bool parsed_changeset<IDX>::single_index() const {
    return v_.size() == 1;
}

template <typename IDX>
inline IDX parsed_changeset<IDX>::last_index() const {
    return v_.empty() ? IDX(-1) : v_.back().index();
}

template <typename IDX>
inline auto parsed_changeset<IDX>::begin() const -> iterator {
    return v_.begin();
}

template <typename IDX>
inline auto parsed_changeset<IDX>::end() const -> iterator {
    return v_.end();
}

#endif
