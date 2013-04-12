#ifndef MASSTREE_SERIAL_CHANGESET_HH
#define MASSTREE_SERIAL_CHANGESET_HH
#include <iterator>

template <typename IDX>
class serial_changeset_iterator : public std::iterator<std::input_iterator_tag, serial_changeset_iterator<IDX> > {
  public:
    inline serial_changeset_iterator();
    inline serial_changeset_iterator(const char* x);

    inline bool check(serial_changeset_iterator<IDX> last) const;

    inline IDX index() const;
    inline Str value() const;
    inline int32_t value_length() const;

    inline const serial_changeset_iterator<IDX>& operator*() const;
    inline const serial_changeset_iterator<IDX>* operator->() const;
    inline void operator++();

    inline bool operator==(serial_changeset_iterator<IDX> x) const;
    inline bool operator!=(serial_changeset_iterator<IDX> x) const;

  private:
    const char* p_;
};

template <typename IDX>
class serial_changeset {
  public:
    inline serial_changeset();
    inline serial_changeset(Str str);

    inline bool empty() const;
    inline bool single_index() const;
    inline IDX last_index() const;

    inline serial_changeset_iterator<IDX> begin() const;
    inline serial_changeset_iterator<IDX> end() const;

  private:
    Str str_;
};


template <typename IDX>
inline serial_changeset_iterator<IDX>::serial_changeset_iterator()
    : p_() {
}

template <typename IDX>
inline serial_changeset_iterator<IDX>::serial_changeset_iterator(const char* x)
    : p_(x) {
}

template <typename IDX>
inline const serial_changeset_iterator<IDX>& serial_changeset_iterator<IDX>::operator*() const {
    return *this;
}

template <typename IDX>
inline const serial_changeset_iterator<IDX>* serial_changeset_iterator<IDX>::operator->() const {
    return this;
}

template <typename IDX>
inline IDX serial_changeset_iterator<IDX>::index() const {
#if HAVE_INDIFFERENT_ALIGNMENT
    return *reinterpret_cast<const IDX*>(p_);
#else
    IDX idx;
    memcpy(&idx, p_, sizeof(IDX));
    return idx;
#endif
}

template <typename IDX>
inline int32_t serial_changeset_iterator<IDX>::value_length() const {
#if HAVE_INDIFFERENT_ALIGNMENT
    return *reinterpret_cast<const int32_t*>(p_ + sizeof(IDX));
#else
    int32_t len;
    memcpy(&len, p_ + sizeof(IDX), sizeof(int32_t));
    return len;
#endif
}

template <typename IDX>
inline Str serial_changeset_iterator<IDX>::value() const {
    return Str(p_ + sizeof(IDX) + sizeof(int32_t), value_length());
}

template <typename IDX>
inline bool serial_changeset_iterator<IDX>::check(serial_changeset_iterator<IDX> last) const {
    int32_t len;
    return last.p_ - p_ >= ssize_t(sizeof(IDX) + sizeof(int32_t))
	&& (len = value_length()) >= 0
	&& last.p_ - (p_ + sizeof(IDX) + sizeof(int32_t)) >= len;
}

template <typename IDX>
inline void serial_changeset_iterator<IDX>::operator++() {
    p_ += sizeof(IDX) + sizeof(int32_t) + value_length();
}

template <typename IDX>
inline bool serial_changeset_iterator<IDX>::operator==(serial_changeset_iterator<IDX> x) const {
    return p_ == x.p_;
}

template <typename IDX>
inline bool serial_changeset_iterator<IDX>::operator!=(serial_changeset_iterator<IDX> x) const {
    return p_ != x.p_;
}

template <typename IDX>
inline serial_changeset<IDX>::serial_changeset()
    : str_() {
}

template <typename IDX>
inline serial_changeset<IDX>::serial_changeset(Str str)
    : str_(str) {
    serial_changeset_iterator<IDX> ib(str_.begin()), ie(str_.end());
    while (ib != ie && ib.check(ie))
	++ib;
    if (ib != ie)
	str_ = Str();
}

template <typename IDX>
inline bool serial_changeset<IDX>::empty() const {
    return str_.empty();
}

template <typename IDX>
inline bool serial_changeset<IDX>::single_index() const {
    return !str_.empty()
        && (serial_changeset_iterator<IDX>(str_.begin()).value_length()
            == str_.length() - int(sizeof(IDX) + sizeof(int32_t)));
}

template <typename IDX>
inline IDX serial_changeset<IDX>::last_index() const {
    IDX idx = IDX(-1);
    auto endit = end();
    for (auto it = begin(); it != endit; ++it)
        idx = it.index();
    return idx;
}

template <typename IDX>
inline serial_changeset_iterator<IDX> serial_changeset<IDX>::begin() const {
    return serial_changeset_iterator<IDX>(str_.begin());
}

template <typename IDX>
inline serial_changeset_iterator<IDX> serial_changeset<IDX>::end() const {
    return serial_changeset_iterator<IDX>(str_.end());
}

#endif
