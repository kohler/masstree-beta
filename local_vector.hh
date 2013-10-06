#ifndef GSTORE_LOCAL_VECTOR_HH
#define GSTORE_LOCAL_VECTOR_HH 1
#include "compiler.hh"
#include <memory>
#include <iterator>

template <typename T, int N, typename A = std::allocator<T> >
class local_vector {
  public:
    typedef bool (local_vector<T, N, A>::*unspecified_bool_type)() const;
    typedef T* iterator;
    typedef const T* const_iterator;
    typedef std::reverse_iterator<iterator> reverse_iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;
    typedef unsigned size_type;

    inline local_vector(const A& allocator = A());
    local_vector(const local_vector<T, N, A>& x);
    template <int NN, typename AA>
    local_vector(const local_vector<T, NN, AA>& x);
    inline ~local_vector();

    inline size_type size() const;
    inline bool empty() const;
    inline operator unspecified_bool_type() const;
    inline bool operator!() const;

    inline iterator begin();
    inline iterator end();
    inline const_iterator begin() const;
    inline const_iterator end() const;
    inline const_iterator cbegin() const;
    inline const_iterator cend() const;
    inline reverse_iterator rbegin();
    inline reverse_iterator rend();
    inline const_reverse_iterator rbegin() const;
    inline const_reverse_iterator rend() const;
    inline const_reverse_iterator crbegin() const;
    inline const_reverse_iterator crend() const;

    inline T& operator[](size_type i);
    inline const T& operator[](size_type i) const;
    inline T& front();
    inline const T& front() const;
    inline T& back();
    inline const T& back() const;

    inline void clear();
    inline void push_back(const T& x);
    inline void push_back(T&& x);
    template <typename... Args> inline void emplace_back(Args&&... args);
    inline void pop_back();

    inline local_vector<T, N, A>& operator=(const local_vector<T, N, A>& x);
    template <int NN, typename AA>
    inline local_vector<T, N, A>& operator=(const local_vector<T, NN, AA>& x);

  private:
    struct rep : public A {
	T* v_;
	size_type size_;
	size_type capacity_;
	char lv_[sizeof(T) * N];

	inline rep(const A& a);
    };
    rep r_;

    void grow();
};

template <typename T, int N, typename A>
inline local_vector<T, N, A>::rep::rep(const A& a)
    : A(a), v_(reinterpret_cast<T*>(lv_)), size_(0), capacity_(N) {
}

template <typename T, int N, typename A>
inline local_vector<T, N, A>::local_vector(const A& allocator)
    : r_(allocator) {
}

template <typename T, int N, typename A>
local_vector<T, N, A>::local_vector(const local_vector<T, N, A>& x)
    : r_(A()) {
    for (size_type i = 0; i != x.r_.size_; ++i)
        push_back(x.r_.v_[i]);
}

template <typename T, int N, typename A> template <int NN, typename AA>
local_vector<T, N, A>::local_vector(const local_vector<T, NN, AA>& x)
    : r_(A()) {
    for (size_type i = 0; i != x.r_.size_; ++i)
        push_back(x.r_.v_[i]);
}

template <typename T, int N, typename A>
inline local_vector<T, N, A>::~local_vector() {
    for (size_type i = 0; i != r_.size_; ++i)
	r_.destroy(&r_.v_[i]);
    if (r_.v_ != reinterpret_cast<T*>(r_.lv_))
	r_.deallocate(r_.v_, r_.capacity_);
}

template <typename T, int N, typename A>
inline unsigned local_vector<T, N, A>::size() const {
    return r_.size_;
}

template <typename T, int N, typename A>
inline bool local_vector<T, N, A>::empty() const {
    return r_.size_ == 0;
}

template <typename T, int N, typename A>
inline local_vector<T, N, A>::operator unspecified_bool_type() const {
    return empty() ? 0 : &local_vector<T, N, A>::empty;
}

template <typename T, int N, typename A>
inline bool local_vector<T, N, A>::operator!() const {
    return empty();
}

template <typename T, int N, typename A>
void local_vector<T, N, A>::grow() {
    T* m = r_.allocate(r_.capacity_ * 2);
    for (size_type i = 0; i != r_.size_; ++i) {
	r_.construct(&m[i], std::move(r_.v_[i]));
	r_.destroy(&r_.v_[i]);
    }
    if (r_.v_ != reinterpret_cast<T*>(r_.lv_))
	r_.deallocate(r_.v_, r_.capacity_);
    r_.v_ = m;
    r_.capacity_ *= 2;
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::begin() -> iterator {
    return r_.v_;
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::end() -> iterator {
    return r_.v_ + r_.size_;
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::begin() const -> const_iterator {
    return r_.v_;
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::end() const -> const_iterator {
    return r_.v_ + r_.size_;
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::cbegin() const -> const_iterator {
    return r_.v_;
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::cend() const -> const_iterator {
    return r_.v_ + r_.size_;
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::rbegin() -> reverse_iterator {
    return reverse_iterator(end());
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::rend() -> reverse_iterator {
    return reverse_iterator(begin());
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::rbegin() const -> const_reverse_iterator {
    return const_reverse_iterator(end());
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::rend() const -> const_reverse_iterator {
    return const_reverse_iterator(begin());
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::crbegin() const -> const_reverse_iterator {
    return const_reverse_iterator(end());
}

template <typename T, int N, typename A>
inline auto local_vector<T, N, A>::crend() const -> const_reverse_iterator {
    return const_reverse_iterator(begin());
}

template <typename T, int N, typename A>
inline T& local_vector<T, N, A>::operator[](size_type i) {
    return r_.v_[i];
}

template <typename T, int N, typename A>
inline const T& local_vector<T, N, A>::operator[](size_type i) const {
    return r_.v_[i];
}

template <typename T, int N, typename A>
inline T& local_vector<T, N, A>::front() {
    return r_.v_[0];
}

template <typename T, int N, typename A>
inline const T& local_vector<T, N, A>::front() const {
    return r_.v_[0];
}

template <typename T, int N, typename A>
inline T& local_vector<T, N, A>::back() {
    return r_.v_[r_.size_ - 1];
}

template <typename T, int N, typename A>
inline const T& local_vector<T, N, A>::back() const {
    return r_.v_[r_.size_ - 1];
}

template <typename T, int N, typename A>
inline void local_vector<T, N, A>::push_back(const T& x) {
    if (r_.size_ == r_.capacity_)
	grow();
    r_.construct(&r_.v_[r_.size_], x);
    ++r_.size_;
}

template <typename T, int N, typename A>
inline void local_vector<T, N, A>::push_back(T&& x) {
    if (r_.size_ == r_.capacity_)
	grow();
    r_.construct(&r_.v_[r_.size_], std::move(x));
    ++r_.size_;
}

template <typename T, int N, typename A> template <typename... Args>
inline void local_vector<T, N, A>::emplace_back(Args&&... args) {
    if (r_.size_ == r_.capacity_)
	grow();
    r_.construct(&r_.v_[r_.size_], std::forward<Args>(args)...);
    ++r_.size_;
}

template <typename T, int N, typename A>
inline void local_vector<T, N, A>::pop_back() {
    assert(r_.size_ != 0);
    --r_.size_;
    r_.destroy(&r_.v_[r_.size_]);
}

template <typename T, int N, typename A>
inline void local_vector<T, N, A>::clear() {
    for (size_type i = 0; i != r_.size_; ++i)
        r_.destroy(&r_.v_[i]);
    r_.size_ = 0;
}

template <typename T, int N, typename A>
local_vector<T, N, A>&
local_vector<T, N, A>::operator=(const local_vector<T, N, A>& x) {
    if (&x != this) {
        clear();
        for (size_type i = 0; i != x.r_.size_; ++i)
            push_back(x.r_.v_[i]);
    }
    return *this;
}

template <typename T, int N, typename A> template <int NN, typename AA>
local_vector<T, N, A>&
local_vector<T, N, A>::operator=(const local_vector<T, NN, AA>& x) {
    clear();
    for (size_type i = 0; i != x.r_.size_; ++i)
        push_back(x.r_.v_[i]);
    return *this;
}

#endif
