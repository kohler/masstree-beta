#ifndef FIXSIZED_KEY_HH
#define FIXSIZED_KEY_HH
#include "str.hh"
#include "string.hh"
#include "string_slice.hh"

using lcdf::Str;
using lcdf::String;

template <size_t Size, bool IntCmp = true>
class fix_sized_key {
  constexpr static bool int_cmp = IntCmp;
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

    fix_sized_key(ikey_type k0, ikey_type k1) {
        memset(ikey_u.s, 0, ikey_size);
        ikey_u.ikey[0] = k0;
        ikey_u.ikey[1] = k1;
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

    int compare(const fix_sized_key<Size, IntCmp> x) const {
        return comparator<IntCmp>::compare(*this, x);
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
//   private:
    union union_type {
        ikey_type ikey[ikey_size / sizeof(ikey_type)];
        char s[ikey_size];
    } ikey_u;

    template<bool IntCmp_> struct comparator;
    template<> struct comparator<true> {
        using key = fix_sized_key<Size, true>;
        static int compare(const key& x, const key& y) {
            int cmp = ::compare(x.ikey_u.ikey[1], y.ikey_u.ikey[1]);
            if (cmp == 0) {
                cmp = ::compare(x.ikey_u.ikey[0], y.ikey_u.ikey[0]);
            }
            return cmp;
        }
    };
    template<> struct comparator<false> {
        using key = fix_sized_key<Size, false>;
        static int compare(const key& x, const key& y) {
            return strcmp(x->ikey_u.s, y.ikey_u.s);
        }
    };

};

template <size_t S, bool C> constexpr int fix_sized_key<S, C>::ikey_size;

template <size_t S, bool C>
inline std::ostream& operator<<(std::ostream& stream,
                                const fix_sized_key<S, C>& x) {
    return stream << x.unparse();
}

#endif
