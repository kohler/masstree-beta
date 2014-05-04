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
#ifndef STRINGBAG_HH
#define STRINGBAG_HH 1
#include "compiler.hh"
#include "string_slice.hh"

/** */
template <typename L>
class stringbag {
    struct info_type {
        L pos;
        L len;
        info_type(int p, int l)
            : pos(p), len(l) {
        }
    };
    typedef string_slice<uintptr_t> slice_type;

  public:

    stringbag(int width, size_t allocated_size) {
        size_t firstpos = overhead(width);
        assert(allocated_size >= firstpos
               && allocated_size <= (size_t) (L) -1);
        size_ = firstpos;
        allocated_size_ = allocated_size;
        memset(info_, 0, sizeof(info_type) * width);
    }

    static size_t overhead(int width) {
        return sizeof(stringbag<L>) + width * sizeof(info_type);
    }

    int size() const {
        return size_;
    }
    int allocated_size() const {
        return allocated_size_;
    }

    lcdf::Str get(int p) const {
        info_type info = info_[p];
        return lcdf::Str(s_ + info.pos, info.len);
    }

    bool equals_sloppy(int p, const char *s, int len) const {
        info_type info = info_[p];
        if (info.len != len)
            return false;
        else
            return slice_type::equals_sloppy(s, s_ + info.pos, len);
    }
    bool equals_sloppy(int p, lcdf::Str s) const {
        return equals_sloppy(p, s.s, s.len);
    }
    bool equals(int p, const char *s, int len) const {
        info_type info = info_[p];
        return info.len == len
            && memcmp(s_ + info.pos, s, len) == 0;
    }
    bool equals(int p, lcdf::Str s) const {
        return equals(p, s.s, s.len);
    }

    int compare(int p, const char *s, int len) const {
        info_type info = info_[p];
        int minlen = std::min(len, (int) info.len);
        int cmp = memcmp(s_ + info.pos, s, minlen);
        return cmp ? cmp : ::compare((int) info.len, len);
    }
    int compare(int p, lcdf::Str s) const {
        return compare(p, s.s, s.len);
    }

    bool assign(int p, const char *s, int len) {
        int pos, mylen = info_[p].len;
        if (mylen >= len)
            pos = info_[p].pos;
        else if (size() + std::max(len, slice_type::size)
                   <= allocated_size()) {
            pos = size();
            size_ += len;
        } else
            return false;
        memcpy(s_ + pos, s, len);
        info_[p] = info_type(pos, len);
        return true;
    }
    bool assign(int p, lcdf::Str s) {
        return assign(p, s.s, s.len);
    }

    void print(int width, FILE *f, const char *prefix, int indent) {
        fprintf(f, "%s%*s%p (%d:)%d:%d...\n", prefix, indent, "",
                this, (int) overhead(width), size(), allocated_size());
        for (int i = 0; i < width; ++i)
            if (info_[i].len)
                fprintf(f, "%s%*s  #%x %d:%d %.*s\n", prefix, indent, "",
                        i, info_[i].pos, info_[i].len, std::min((int) info_[i].len, 40), s_ + info_[i].pos);
    }

  private:
    union {
        struct {
            L size_;
            L allocated_size_;
            info_type info_[0];
        };
        char s_[0];
    };
};

#endif
