#ifndef FIXSIZED_KEY_HH
#define FIXSIZED_KEY_HH
#include "str.hh"
#include "string.hh"
#include "string_slice.hh"

using lcdf::Str;
using lcdf::String;

template <size_t Size>
class fix_sized_key {
  public:

    // static constexpr int nikey = 1;
    /** @brief Type of ikeys. */
    using ikey_type = uint64_t;

    /** @brief Size of ikeys in bytes. */
    static constexpr int ikey_size = Size;

    /** @brief Construct an uninitialized key. */
    fix_sized_key() {
        memset(ikey_u.s, 0, ikey_size);
    }
    /** @brief Construct a key for string @a s. */
    fix_sized_key(Str s) {
        memset(ikey_u.s, 0, ikey_size);
        memcpy(ikey_u.s, s.data(), std::min(s.length(), ikey_size));
    }
    /** @brief Test if this key is empty (holds the empty string). */
    bool empty() const {
        return ikey_u.ikey[0] == 0 && ikey_u.ikey[1] == 0;
    }
    /** @brief Return the ikey. */
    /** @brief Return the key's length. */
    int length() const {
        return strlen(this->ikey_u.s);
    }
    int compare(const fix_sized_key<Size>& x) const {
        int cmp = ::compare(this->ikey_u.ikey[1], x.ikey_u.ikey[1]);
        if (cmp == 0) {
            cmp = ::compare(this->ikey_u.ikey[0], x.ikey_u.ikey[0]);
        }
        return cmp;
    }

    String unparse() const {
        return String(ikey_u.s);
    }
    String unparse_printable() const {
        return unparse().printable();
    }
    static String unparse_ikey(ikey_type ikey) {
        fix_sized_key<Size> k(ikey);
        return k.unparse();
    }
    static String unparse_printable_ikey(ikey_type ikey) {
        fix_sized_key<Size> k(ikey);
        return k.unparse_printable();
    }

    Str full_string() const {
        return Str(ikey_u.s);
    }
    operator Str() const {
        return full_string();
    }
  private:
    union union_type {
        ikey_type ikey[ikey_size / sizeof(ikey_type)];
        char s[ikey_size];
    } ikey_u;
};

template <size_t S> constexpr int fix_sized_key<S>::ikey_size;

template <size_t S>
inline std::ostream& operator<<(std::ostream& stream,
                                const fix_sized_key<S>& x) {
    return stream << x.unparse();
}

#endif
