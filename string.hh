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
#ifndef LCDF_STRING_HH
#define LCDF_STRING_HH 1
#include "string_base.hh"

class String : public String_base<String> {
  public:
    typedef String substring_type;

    inline String();
    inline String(const String &x);
#if HAVE_CXX_RVALUE_REFERENCES
    inline String(String &&x);
#endif
    template <typename T>
    explicit inline String(const String_base<T> &x);
    inline String(const char *cstr);
    inline String(const char *s, int len);
    inline String(const unsigned char *s, int len);
    inline String(const char *first, const char *last);
    inline String(const unsigned char *first, const unsigned char *last);
    explicit inline String(bool x);
    explicit inline String(char c);
    explicit inline String(unsigned char c);
    explicit String(int x);
    explicit String(unsigned x);
    explicit String(long x);
    explicit String(unsigned long x);
    explicit String(long long x);
    explicit String(unsigned long long x);
    explicit String(double x);
    inline ~String();

    static inline const String &make_empty();
    static inline const String &make_out_of_memory();
    static String make_uninitialized(int len);
    static inline String make_stable(const char *cstr);
    static inline String make_stable(const char *s, int len);
    static inline String make_stable(const char *first, const char *last);
    static String make_fill(int c, int n); // n copies of c
    static inline const String &make_zero();

    inline const char *data() const;
    inline int length() const;

    inline const char *c_str() const;

    inline String substring(const char *first, const char *last) const;
    inline String fast_substring(const char *first, const char *last) const;
    String substring(int pos, int len) const;
    inline String substring(int pos) const;
    String ltrim() const;
    String rtrim() const;
    String trim() const;

    String lower() const;
    String upper() const;
    String printable(int type = 0) const;

    enum {
	utf_strip_bom = 1,
	utf_replacement = 2,
	utf_prefer_le = 4
    };

    enum {
	u_replacement = 0xFFFD
    };

    String windows1252_to_utf8() const;
    String utf16be_to_utf8(int flags = 0) const;
    String utf16le_to_utf8(int flags = 0) const;
    String utf16_to_utf8(int flags = 0) const;
    String cesu8_to_utf8(int flags = 0) const;
    String utf8_to_utf8(int flags = 0) const;
    String to_utf8(int flags = 0) const;

    String encode_json() const;

    inline String &operator=(const String &x);
#if HAVE_CXX_RVALUE_REFERENCES
    inline String &operator=(String &&x);
#endif
    template <typename T>
    inline String &operator=(const String_base<T> &str);
    inline String &operator=(const char *cstr);

    inline void swap(String &x);

    inline void append(const String &x);
    inline void append(const char *cstr);
    inline void append(const char *s, int len);
    inline void append(const char *first, const char *last);
    void append_fill(int c, int len);
    char *append_uninitialized(int len);

    inline String &operator+=(const String &x);
    template <typename T>
    inline String &operator+=(const String_base<T> &x);
    inline String &operator+=(const char *cstr);
    inline String &operator+=(char c);

    // String operator+(String, const String &);
    // String operator+(String, const char *);
    // String operator+(const char *, const String &);

    inline String compact() const;

    inline bool data_shared() const;
    char *mutable_data();
    inline unsigned char *mutable_udata();
    char *mutable_c_str();

    void align(int);

    static const unsigned char *skip_utf8_char(const unsigned char *first,
					       const unsigned char *last);
    static const char *skip_utf8_char(const char *first, const char *last);
    static const unsigned char *skip_utf8_bom(const unsigned char *first,
					      const unsigned char *last);
    static const char *skip_utf8_bom(const char *first, const char *last);

  private:

    /** @cond never */
    struct memo_t {
	volatile uint32_t refcount;
	uint32_t capacity;
	volatile uint32_t dirty;
#if HAVE_STRING_PROFILING > 1
	memo_t **pprev;
	memo_t *next;
#endif
	char real_data[8];	// but it might be more or less
    };

    enum {
	MEMO_SPACE = sizeof(memo_t) - 8
    };

    struct rep_t {
	const char *data;
	int length;
	memo_t *memo;
    };
    /** @endcond never */

    mutable rep_t _r;		// mutable for c_str()

#if HAVE_STRING_PROFILING
    static uint64_t live_memo_count;
    static uint64_t memo_sizes[55];
    static uint64_t live_memo_sizes[55];
    static uint64_t live_memo_bytes[55];
# if HAVE_STRING_PROFILING > 1
    static memo_t *live_memos[55];
# endif

    static inline int profile_memo_size_bucket(uint32_t dirty, uint32_t capacity) {
	if (capacity <= 16)
	    return dirty;
	else if (capacity <= 32)
	    return 17 + (capacity - 17) / 2;
	else if (capacity <= 64)
	    return 25 + (capacity - 33) / 8;
	else
	    return 29 + 26 - ffs_msb(capacity - 1);
    }

    static void profile_update_memo_dirty(memo_t *memo, uint32_t old_dirty, uint32_t new_dirty, uint32_t capacity) {
	if (capacity <= 16 && new_dirty != old_dirty) {
	    ++memo_sizes[new_dirty];
	    ++live_memo_sizes[new_dirty];
	    live_memo_bytes[new_dirty] += capacity;
	    --live_memo_sizes[old_dirty];
	    live_memo_bytes[old_dirty] -= capacity;
# if HAVE_STRING_PROFILING > 1
	    if ((*memo->pprev = memo->next))
		memo->next->pprev = memo->pprev;
	    memo->pprev = &live_memos[new_dirty];
	    if ((memo->next = *memo->pprev))
		memo->next->pprev = &memo->next;
	    *memo->pprev = memo;
# else
	    (void) memo;
# endif
	}
    }

    static void one_profile_report(StringAccum &sa, int i, int examples);
#endif

    inline void assign_memo(const char *data, int length, memo_t *memo) const {
	_r.data = data;
	_r.length = length;
	if ((_r.memo = memo))
	    ++memo->refcount;
    }

    inline String(const char *data, int length, memo_t *memo) {
	assign_memo(data, length, memo);
    }

    inline void assign(const String &x) const {
	assign_memo(x._r.data, x._r.length, x._r.memo);
    }

    inline void deref() const {
	if (_r.memo && --_r.memo->refcount == 0)
	    delete_memo(_r.memo);
    }

    void assign(const char *s, int len, bool need_deref);
    void assign_out_of_memory();
    void append(const char *s, int len, memo_t *memo);
    static String hard_make_stable(const char *s, int len);
    static inline memo_t *absent_memo() {
	return reinterpret_cast<memo_t *>(uintptr_t(1));
    }
    static memo_t *create_memo(char *space, int dirty, int capacity);
    static void delete_memo(memo_t *memo);
    const char *hard_c_str() const;
    bool hard_equals(const char *s, int len) const;

    static const char int_data[20];
    static const rep_t null_string_rep;
    static const rep_t oom_string_rep;
    static const rep_t zero_string_rep;

    static String make_claim(char *, int, int); // claim memory

    static int parse_cesu8_char(const unsigned char *s,
				const unsigned char *end);

    friend struct rep_t;
    friend class StringAccum;

};


/** @brief Construct an empty String (with length 0). */
inline String::String() {
    assign_memo(String_generic::empty_data, 0, 0);
}

/** @brief Construct a copy of the String @a x. */
inline String::String(const String &x) {
    assign(x);
}

#if HAVE_CXX_RVALUE_REFERENCES
/** @brief Move-construct a String from @a x. */
inline String::String(String &&x)
    : _r(x._r) {
    x._r.memo = 0;
}
#endif

/** @brief Construct a copy of the string @a x. */
template <typename T>
inline String::String(const String_base<T> &x) {
    assign(x.data(), x.length(), false);
}

/** @brief Construct a String containing the C string @a cstr.
    @param cstr a null-terminated C string
    @return A String containing the characters of @a cstr, up to but not
    including the terminating null character. */
inline String::String(const char *cstr) {
    if (LCDF_CONSTANT_CSTR(cstr))
	assign_memo(cstr, strlen(cstr), 0);
    else
	assign(cstr, -1, false);
}

/** @brief Construct a String containing the first @a len characters of
    string @a s.
    @param s a string
    @param len number of characters to take from @a s.  If @a len @< 0,
    then takes @c strlen(@a s) characters.
    @return A String containing @a len characters of @a s. */
inline String::String(const char *s, int len) {
    assign(s, len, false);
}

/** @overload */
inline String::String(const unsigned char *s, int len) {
    assign(reinterpret_cast<const char *>(s), len, false);
}

/** @brief Construct a String containing the characters from @a first
    to @a last.
    @param first first character in string (begin iterator)
    @param last pointer one past last character in string (end iterator)
    @return A String containing the characters from @a first to @a last.

    Constructs an empty string if @a first @>= @a last. */
inline String::String(const char *first, const char *last) {
    assign(first, (first < last ? last - first : 0), false);
}

/** @overload */
inline String::String(const unsigned char *first, const unsigned char *last) {
    assign(reinterpret_cast<const char *>(first),
	   (first < last ? last - first : 0), false);
}

/** @brief Construct a String equal to "true" or "false" depending on the
    value of @a x. */
inline String::String(bool x) {
    // bool_data equals "false\0true\0"
    assign_memo(String_generic::bool_data + (-x & 6), 5 - x, 0);
}

/** @brief Construct a String containing the single character @a c. */
inline String::String(char c) {
    assign(&c, 1, false);
}

/** @overload */
inline String::String(unsigned char c) {
    assign(reinterpret_cast<char *>(&c), 1, false);
}

/** @brief Destroy a String, freeing memory if necessary. */
inline String::~String() {
    deref();
}

/** @brief Return a const reference to an empty String.

    May be quicker than String::String(). */
inline const String &String::make_empty() {
    return reinterpret_cast<const String &>(null_string_rep);
}

/** @brief Return a String containing @a len unknown characters. */
inline String String::make_uninitialized(int len) {
    String s;
    s.append_uninitialized(len);
    return s;
}

/** @brief Return a const reference to the string "0". */
inline const String &String::make_zero() {
    return reinterpret_cast<const String &>(zero_string_rep);
}

/** @brief Return a String that directly references the C string @a cstr.

    The make_stable() functions are suitable for static constant strings
    whose data is known to stay around forever, such as C string constants.

    @warning The String implementation may access @a cstr's terminating null
    character. */
inline String String::make_stable(const char *cstr) {
    if (LCDF_CONSTANT_CSTR(cstr))
	return String(cstr, strlen(cstr), 0);
    else
	return hard_make_stable(cstr, -1);
}

/** @brief Return a String that directly references the first @a len
    characters of @a s.

    If @a len @< 0, treats @a s as a null-terminated C string.

    @warning The String implementation may access @a s[@a len], which
    should remain constant even though it's not part of the String. */
inline String String::make_stable(const char *s, int len) {
    if (__builtin_constant_p(len) && len >= 0)
	return String(s, len, 0);
    else
	return hard_make_stable(s, len);
}

/** @brief Return a String that directly references the character data in
    [@a first, @a last).
    @param first pointer to the first character in the character data
    @param last pointer one beyond the last character in the character data
    (but see the warning)

    This function is suitable for static constant strings whose data is
    known to stay around forever, such as C string constants.  Returns an
    empty string if @a first @>= @a last.

    @warning The String implementation may access *@a last, which should
    remain constant even though it's not part of the String. */
inline String String::make_stable(const char *first, const char *last) {
    return String(first, (first < last ? last - first : 0), 0);
}

/** @brief Return a pointer to the string's data.

    Only the first length() characters are valid, and the string
    might not be null-terminated. */
inline const char *String::data() const {
    return _r.data;
}

/** @brief Return the string's length. */
inline int String::length() const {
    return _r.length;
}

/** @brief Null-terminate the string.

    The terminating null character isn't considered part of the string, so
    this->length() doesn't change.  Returns a corresponding C string
    pointer.  The returned pointer is semi-temporary; it will persist until
    the string is destroyed or appended to. */
inline const char *String::c_str() const {
    // See also hard_c_str().
#if HAVE_OPTIMIZE_SIZE || __OPTIMIZE_SIZE__
    return hard_c_str();
#else
    // We may already have a '\0' in the right place.  If _memo has no
    // capacity, then this is one of the special strings (null or
    // stable). We are guaranteed, in these strings, that _data[_length]
    // exists. Otherwise must check that _data[_length] exists.
    const char *end_data = _r.data + _r.length;
    if ((_r.memo && end_data >= _r.memo->real_data + _r.memo->dirty)
	|| *end_data != '\0') {
	if (char *x = const_cast<String *>(this)->append_uninitialized(1)) {
	    *x = '\0';
	    --_r.length;
	}
    }
    return _r.data;
#endif
}

/** @brief Return a substring of the current string starting at @a first
    and ending before @a last.
    @param first pointer to the first substring character
    @param last pointer one beyond the last substring character

    Returns an empty string if @a first @>= @a last. Also returns an empty
    string if @a first or @a last is out of range (i.e., either less than
    this->begin() or greater than this->end()), but this should be
    considered a programming error; a future version may generate a warning
    for this case. */
inline String String::substring(const char *first, const char *last) const {
    if (first < last && first >= _r.data && last <= _r.data + _r.length)
	return String(first, last - first, _r.memo);
    else
	return String();
}

/** @brief Return a substring of the current string starting at @a first
    and ending before @a last.
    @param first pointer to the first substring character
    @param last pointer one beyond the last substring character
    @pre begin() <= @a first <= @a last <= end() */
inline String String::fast_substring(const char *first, const char *last) const {
    assert(begin() <= first && first <= last && last <= end());
    return String(first, last - first, _r.memo);
}

/** @brief Return the suffix of the current string starting at index @a pos.

    If @a pos is negative, starts that far from the end of the string.
    If @a pos is so negative that the suffix starts outside the string,
    then the entire string is returned. If the substring is beyond the
    end of the string (@a pos > length()), returns an empty string (but
    this should be considered a programming error; a future version may
    generate a warning for this case).

    @note String::substring() is intended to behave like Perl's
    substr(). */
inline String String::substring(int pos) const {
    return substring((pos <= -_r.length ? 0 : pos), _r.length);
}

/** @brief Assign this string to @a x. */
inline String &String::operator=(const String &x) {
    if (likely(&x != this)) {
	deref();
	assign(x);
    }
    return *this;
}

#if HAVE_CXX_RVALUE_REFERENCES
/** @brief Move-assign this string to @a x. */
inline String &String::operator=(String &&x) {
    deref();
    _r = x._r;
    x._r.memo = 0;
    return *this;
}
#endif

/** @brief Assign this string to the C string @a cstr. */
inline String &String::operator=(const char *cstr) {
    if (LCDF_CONSTANT_CSTR(cstr)) {
	deref();
	assign_memo(cstr, strlen(cstr), 0);
    } else
	assign(cstr, -1, true);
    return *this;
}

/** @brief Assign this string to the C string @a cstr. */
template <typename T>
inline String &String::operator=(const String_base<T> &str) {
    assign(str.data(), str.length(), true);
    return *this;
}

/** @brief Swap the values of this string and @a x. */
inline void String::swap(String &x) {
    rep_t r = _r;
    _r = x._r;
    x._r = r;
}

/** @brief Append @a x to this string. */
inline void String::append(const String &x) {
    append(x.data(), x.length(), x._r.memo);
}

/** @brief Append the null-terminated C string @a cstr to this string.
    @param cstr data to append */
inline void String::append(const char *cstr) {
    if (LCDF_CONSTANT_CSTR(cstr))
	append(cstr, strlen(cstr), absent_memo());
    else
	append(cstr, -1, absent_memo());
}

/** @brief Append the first @a len characters of @a s to this string.
    @param s data to append
    @param len length of data

    If @a len @< 0, treats @a s as a null-terminated C string. */
inline void String::append(const char *s, int len) {
    append(s, len, absent_memo());
}

/** @brief Appends the data from @a first to @a last to this string.

    Does nothing if @a first @>= @a last. */
inline void String::append(const char *first, const char *last) {
    if (first < last)
	append(first, last - first);
}

/** @brief Append @a x to this string.
    @return *this */
inline String &String::operator+=(const String &x) {
    append(x.data(), x.length(), x._r.memo);
    return *this;
}

/** @brief Append the null-terminated C string @a cstr to this string.
    @return *this */
inline String &String::operator+=(const char *cstr) {
    append(cstr);
    return *this;
}

/** @brief Append the character @a c to this string.
    @return *this */
inline String &String::operator+=(char c) {
    append(&c, 1);
    return *this;
}

/** @brief Append the string @a x to this string.
    @return *this */
template <typename T>
inline String &String::operator+=(const String_base<T> &x) {
    append(x.data(), x.length());
    return *this;
}

/** @brief Test if the String's data is shared or immutable. */
inline bool String::data_shared() const {
    return !_r.memo || _r.memo->refcount != 1;
}

/** @brief Return a compact version of this String.

    The compact version shares no more than 256 bytes of data with any other
    non-stable String. */
inline String String::compact() const {
    if (!_r.memo || _r.memo->refcount == 1
	|| (uint32_t) _r.length + 256 >= _r.memo->capacity)
	return *this;
    else
	return String(_r.data, _r.data + _r.length);
}

/** @brief Return the unsigned char * version of mutable_data(). */
unsigned char *String::mutable_udata() {
    return reinterpret_cast<unsigned char *>(mutable_data());
}

/** @brief Return a const reference to a canonical out-of-memory String. */
inline const String &String::make_out_of_memory() {
    return reinterpret_cast<const String &>(oom_string_rep);
}

/** @brief Return a pointer to the next character in UTF-8 encoding.
    @pre @a first @< @a last

    If @a first doesn't point at a valid UTF-8 character, returns @a first. */
inline const char *String::skip_utf8_char(const char *first, const char *last) {
    return reinterpret_cast<const char *>(
	skip_utf8_char(reinterpret_cast<const unsigned char *>(first),
		       reinterpret_cast<const unsigned char *>(last)));
}

inline const unsigned char *String::skip_utf8_bom(const unsigned char *first,
						  const unsigned char *last) {
    if (last - first >= 3
	&& first[0] == 0xEF && first[1] == 0xBB && first[2] == 0xBF)
	return first + 3;
    else
	return first;
}

inline const char *String::skip_utf8_bom(const char *first, const char *last) {
    return reinterpret_cast<const char *>(
	skip_utf8_bom(reinterpret_cast<const unsigned char *>(first),
		      reinterpret_cast<const unsigned char *>(last)));
}


/** @relates String
    @brief Concatenate the operands and return the result.

    At most one of the two operands can be a null-terminated C string. */
inline String operator+(String a, const String &b) {
    a += b;
    return a;
}

/** @relates String */
inline String operator+(String a, const char *b) {
    a.append(b);
    return a;
}

/** @relates String */
inline String operator+(const char *a, const String &b) {
    String s1(a);
    s1 += b;
    return s1;
}

/** @relates String
    @brief Concatenate the operands and return the result.

    The second operand is a single character. */
inline String operator+(String a, char b) {
    a.append(&b, 1);
    return a;
}

#endif
