#ifndef GSTORE_MSGPACK_HH
#define GSTORE_MSGPACK_HH
#include "json.hh"
#include "local_vector.hh"
#include "straccum.hh"
#include <vector>
struct kvout;
namespace msgpack {
using lcdf::Json;
using lcdf::Str;
using lcdf::String;
using lcdf::StringAccum;

namespace format {
    enum {
        ffixuint = 0x00, nfixuint = 0x80,
        ffixmap = 0x80, nfixmap = 0x10,
        ffixarray = 0x90, nfixarray = 0x10,
        ffixstr = 0xA0, nfixstr = 0x20,
        fnull = 0xC0,
        ffalse = 0xC2, ftrue = 0xC3,
        fbin8 = 0xC4, fbin16 = 0xC5, fbin32 = 0xC6,
        fext8 = 0xC7, fext16 = 0xC8, fext32 = 0xC9,
        ffloat32 = 0xCA, ffloat64 = 0xCB,
        fuint8 = 0xCC, fuint16 = 0xCD, fuint32 = 0xCE, fuint64 = 0xCF,
        fint8 = 0xD0, fint16 = 0xD1, fint32 = 0xD2, fint64 = 0xD3,
        ffixext1 = 0xD4, ffixext2 = 0xD5, ffixext4 = 0xD6,
        ffixext8 = 0xD7, ffixext16 = 0xD8,
        fstr8 = 0xD9, fstr16 = 0xDA, fstr32 = 0xDB,
        farray16 = 0xDC, farray32 = 0xDD,
        fmap16 = 0xDE, fmap32 = 0xDF,
        ffixnegint = 0xE0, nfixnegint = 0x20,
        nfixint = nfixuint + nfixnegint
    };
}

class compact_unparser {
  public:
    inline uint8_t* unparse_null(uint8_t* s) {
	*s++ = format::fnull;
	return s;
    }
    inline uint8_t* unparse(uint8_t* s, uint32_t x) {
        if (x < 128)
            *s++ = x;
        else if (x < 256) {
            *s++ = format::fuint8;
            *s++ = x;
        } else if (x < 65536) {
            *s++ = format::fuint16;
            write_in_net_order<uint16_t>(s, (uint16_t) x);
            s += 2;
        } else {
            *s++ = format::fuint32;
            write_in_net_order<uint32_t>(s, x);
            s += 4;
        }
        return s;
    }
    inline uint8_t* unparse_small(uint8_t* s, uint32_t x) {
        return unparse(s, x);
    }
    inline uint8_t* unparse_tiny(uint8_t* s, int x) {
        assert((uint32_t) x + format::nfixnegint < format::nfixint);
        *s++ = x;
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, uint64_t x) {
        if (x < 4294967296ULL)
            return unparse(s, (uint32_t) x);
        else {
            *s++ = format::fuint64;
            write_in_net_order<uint64_t>(s, x);
            return s + 8;
        }
    }
    inline uint8_t* unparse(uint8_t* s, int32_t x) {
        if ((uint32_t) x + format::nfixnegint < format::nfixint)
            *s++ = x;
        else if ((uint32_t) x + 128 < 256) {
            *s++ = format::fint8;
            *s++ = x;
        } else if ((uint32_t) x + 32768 < 65536) {
            *s++ = format::fint16;
            write_in_net_order<int16_t>(s, (int16_t) x);
            s += 2;
        } else {
            *s++ = format::fint32;
            write_in_net_order<int32_t>(s, x);
            s += 4;
        }
        return s;
    }
    inline uint8_t* unparse_small(uint8_t* s, int32_t x) {
        return unparse(s, x);
    }
    inline uint8_t* unparse(uint8_t* s, int64_t x) {
        if (x + 2147483648ULL < 4294967296ULL)
            return unparse(s, (int32_t) x);
        else {
            *s++ = format::fint64;
            write_in_net_order<int64_t>(s, x);
            return s + 8;
        }
    }
    inline uint8_t* unparse(uint8_t* s, double x) {
        *s++ = format::ffloat64;
        write_in_net_order<double>(s, x);
        return s + 8;
    }
    inline uint8_t* unparse(uint8_t* s, bool x) {
        *s++ = format::ffalse + x;
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, const char *data, int len) {
        if (len < format::nfixstr)
            *s++ = 0xA0 + len;
        else if (len < 256) {
            *s++ = format::fstr8;
            *s++ = len;
        } else if (len < 65536) {
            *s++ = format::fstr16;
            write_in_net_order<uint16_t>(s, (uint16_t) len);
            s += 2;
        } else {
            *s++ = format::fstr32;
            write_in_net_order<uint32_t>(s, len);
            s += 4;
        }
        memcpy(s, data, len);
        return s + len;
    }
    inline uint8_t* unparse(uint8_t* s, Str x) {
        return unparse(s, x.data(), x.length());
    }
    inline uint8_t* unparse(uint8_t* s, const String& x) {
        return unparse(s, x.data(), x.length());
    }
    template <typename T>
    inline uint8_t* unparse(uint8_t* s, const ::std::vector<T>& x) {
        if (x.size() < format::nfixarray)
            *s++ = format::ffixarray + x.size();
        else if (x.size() < 65536) {
            *s++ = format::farray16;
            write_in_net_order<uint16_t>(s, (uint16_t) x.size());
            s += 2;
        } else {
            *s++ = format::farray32;
            write_in_net_order<uint32_t>(s, (uint32_t) x.size());
            s += 4;
        }
        for (typename ::std::vector<T>::const_iterator it = x.begin();
             it != x.end(); ++it)
            s = unparse(s, *it);
        return s;
    }
    void unparse(StringAccum& sa, const Json& j);
    void unparse(kvout& sa, const Json& j);
    inline String unparse(const Json& j);
};

class fast_unparser {
  public:
    inline uint8_t* unparse_null(uint8_t* s) {
	*s++ = format::fnull;
	return s;
    }
    inline uint8_t* unparse(uint8_t* s, uint32_t x) {
        *s++ = format::fuint8;
        write_in_net_order<uint32_t>(s, x);
        return s + 4;
    }
    inline uint8_t* unparse_small(uint8_t* s, uint32_t x) {
        if (x < 128)
            *s++ = x;
        else {
            *s++ = format::fuint8;
            write_in_net_order<uint32_t>(s, x);
            s += 4;
        }
        return s;
    }
    inline uint8_t* unparse_tiny(uint8_t* s, int x) {
        assert((uint32_t) x + format::nfixnegint < format::nfixint);
        *s++ = x;
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, uint64_t x) {
        *s++ = format::fuint64;
        write_in_net_order<uint64_t>(s, x);
        return s + 8;
    }
    inline uint8_t* unparse(uint8_t* s, int32_t x) {
        *s++ = format::fint32;
        write_in_net_order<int32_t>(s, x);
        return s + 4;
    }
    inline uint8_t* unparse_small(uint8_t* s, int32_t x) {
        if ((uint32_t) x + format::nfixnegint < format::nfixint)
            *s++ = x;
        else {
            *s++ = format::fint32;
            write_in_net_order<int32_t>(s, x);
            s += 4;
        }
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, int64_t x) {
        *s++ = format::fint64;
        write_in_net_order<int64_t>(s, x);
        return s + 8;
    }
    inline uint8_t* unparse(uint8_t* s, double x) {
        *s++ = format::ffloat64;
        write_in_net_order<double>(s, x);
        return s + 8;
    }
    inline uint8_t* unparse(uint8_t* s, bool x) {
        *s++ = format::ffalse + x;
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, const char *data, int len) {
        if (len < format::nfixstr)
            *s++ = 0xA0 + len;
        else {
            *s++ = format::fstr32;
            write_in_net_order<uint32_t>(s, (uint32_t) len);
            s += 4;
        }
        memcpy(s, data, len);
        return s + len;
    }
    inline uint8_t* unparse(uint8_t* s, Str x) {
        return unparse(s, x.data(), x.length());
    }
    inline uint8_t* unparse(uint8_t* s, const String& x) {
        return unparse(s, x.data(), x.length());
    }
    template <typename T>
    inline uint8_t* unparse(uint8_t* s, const ::std::vector<T>& x) {
        if (x.size() < format::nfixarray)
            *s++ = format::ffixarray + x.size();
        else {
            *s++ = format::farray32;
            write_in_net_order<uint32_t>(s, (uint32_t) x.size());
            s += 4;
        }
        for (typename ::std::vector<T>::const_iterator it = x.begin();
             it != x.end(); ++it)
            s = unparse(s, *it);
        return s;
    }
};

class streaming_parser {
  public:
    inline streaming_parser();
    inline void reset();

    inline bool done() const;
    inline bool success() const;
    inline bool error() const;

    inline size_t consume(const char* first, size_t length,
                          const String& str = String());
    inline const char* consume(const char* first, const char* last,
                               const String& str = String());
    const uint8_t* consume(const uint8_t* first, const uint8_t* last,
                           const String& str = String());

    inline Json& result();
    inline const Json& result() const;

  private:
    enum {
        st_final = -2, st_error = -1, st_normal = 0, st_partial = 1,
        st_string = 2
    };
    struct selem {
        Json* jp;
        int size;
    };
    int state_;
    local_vector<selem, 2> stack_;
    String str_;
    Json json_;
    Json jokey_;
};

class parser {
  public:
    explicit inline parser(const uint8_t* s)
        : s_(s), str_() {
    }
    explicit inline parser(const String& str)
        : s_(reinterpret_cast<const uint8_t*>(str.begin())), str_(str) {
    }
    inline const char* position() const {
        return reinterpret_cast<const char*>(s_);
    }
    inline bool try_parse_null() {
	if (*s_ == format::fnull) {
	    ++s_;
	    return true;
	} else
	    return false;
    }
    inline int parse_tiny_int() {
        assert((uint32_t) (int8_t) *s_ + format::nfixnegint < format::nfixint);
        return (int8_t) *s_++;
    }
    inline parser& parse_tiny_int(int& x) {
        x = parse_tiny_int();
        return *this;
    }
    template <typename T>
    inline parser& parse_int(T& x) {
        if ((uint32_t) (int8_t) *s_ + format::nfixnegint < format::nfixint) {
            x = (int8_t) *s_;
            ++s_;
        } else {
            assert((uint32_t) *s_ - format::fuint8 < 8);
            hard_parse_int(x);
        }
        return *this;
    }
    inline parser& parse(int& x) {
        return parse_int(x);
    }
    inline parser& parse(long& x) {
        return parse_int(x);
    }
    inline parser& parse(long long& x) {
        return parse_int(x);
    }
    inline parser& parse(unsigned& x) {
        return parse_int(x);
    }
    inline parser& parse(unsigned long& x) {
        return parse_int(x);
    }
    inline parser& parse(unsigned long long& x) {
        return parse_int(x);
    }
    inline parser& parse(bool& x) {
        assert((uint32_t) *s_ - format::ffalse < 2);
        x = *s_ - format::ffalse;
        ++s_;
        return *this;
    }
    inline parser& parse(double& x) {
        assert(*s_ == format::ffloat64);
        x = read_in_net_order<double>(s_ + 1);
        s_ += 9;
        return *this;
    }
    parser& parse(Str& x);
    parser& parse(String& x);
    template <typename T> parser& parse(::std::vector<T>& x);
  private:
    const uint8_t* s_;
    String str_;
    template <typename T> void hard_parse_int(T& x);
};

template <typename T>
void parser::hard_parse_int(T& x) {
    switch (*s_) {
    case format::fuint8:
        x = s_[1];
        s_ += 2;
        break;
    case format::fuint16:
        x = read_in_net_order<uint16_t>(s_ + 1);
        s_ += 3;
        break;
    case format::fuint32:
        x = read_in_net_order<uint32_t>(s_ + 1);
        s_ += 5;
        break;
    case format::fuint64:
        x = read_in_net_order<uint64_t>(s_ + 1);
        s_ += 9;
        break;
    case format::fint8:
        x = (int8_t) s_[1];
        s_ += 2;
        break;
    case format::fint16:
        x = read_in_net_order<int16_t>(s_ + 1);
        s_ += 3;
        break;
    case format::fint32:
        x = read_in_net_order<int32_t>(s_ + 1);
        s_ += 5;
        break;
    case format::fint64:
        x = read_in_net_order<int64_t>(s_ + 1);
        s_ += 9;
        break;
    }
}

inline String compact_unparser::unparse(const Json& j) {
    StringAccum sa;
    unparse(sa, j);
    return sa.take_string();
}

template <typename T>
parser& parser::parse(::std::vector<T>& x) {
    uint32_t sz;
    if ((uint32_t) *s_ - format::ffixarray < format::nfixarray) {
        sz = *s_ - format::ffixarray;
        ++s_;
    } else if (*s_ == format::farray16) {
        sz = read_in_net_order<uint16_t>(s_ + 1);
        s_ += 3;
    } else {
        assert(*s_ == format::farray32);
        sz = read_in_net_order<uint32_t>(s_ + 1);
        s_ += 5;
    }
    for (; sz != 0; --sz) {
        x.push_back(T());
        parse(x.back());
    }
    return *this;
}

inline streaming_parser::streaming_parser()
    : state_(st_normal) {
}

inline void streaming_parser::reset() {
    state_ = st_normal;
    stack_.clear();
}

inline bool streaming_parser::done() const {
    return state_ < 0;
}

inline bool streaming_parser::success() const {
    return state_ == st_final;
}

inline bool streaming_parser::error() const {
    return state_ == st_error;
}

inline const char* streaming_parser::consume(const char* first,
                                             const char* last,
                                             const String& str) {
    return reinterpret_cast<const char*>
        (consume(reinterpret_cast<const uint8_t*>(first),
                 reinterpret_cast<const uint8_t*>(last), str));
}

inline size_t streaming_parser::consume(const char* first, size_t length,
                                        const String& str) {
    const uint8_t* ufirst = reinterpret_cast<const uint8_t*>(first);
    return consume(ufirst, ufirst + length, str) - ufirst;
}

inline Json& streaming_parser::result() {
    return json_;
}

inline const Json& streaming_parser::result() const {
    return json_;
}

} // namespace msgpack
#endif
