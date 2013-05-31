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
    typedef L info_type;
    static constexpr int max_halfinfo = (1 << (4 * sizeof(info_type))) - 1;

  public:

    stringbag(int width, size_t allocated_size) {
	size_t firstpos = overhead(width);
	assert(allocated_size >= firstpos
	       && allocated_size <= (size_t) max_halfinfo);
	main_ = make_info((int) firstpos, (int) allocated_size);
	memset(info_, 0, sizeof(info_type) * width);
    }

    static size_t overhead(int width) {
	return sizeof(stringbag<L>) + width * sizeof(info_type);
    }

    int size() const {
	return info_pos(main_);
    }
    int allocated_size() const {
	return info_len(main_);
    }

    lcdf::Str get(int p) const {
	info_type info = info_[p];
	return lcdf::Str(s_ + info_pos(info), info_len(info));
    }

    bool equals_sloppy(int p, const char *s, int len) const {
	info_type info = info_[p];
	if (info_len(info) != len)
	    return false;
	else
	    return string_slice<uintptr_t>::equals_sloppy(s, s_ + info_pos(info), len);
    }
    bool equals_sloppy(int p, lcdf::Str s) const {
	return equals_sloppy(p, s.s, s.len);
    }
    bool equals(int p, const char *s, int len) const {
	info_type info = info_[p];
	return info_len(info) == len
	    && memcmp(s_ + info_pos(info), s, len) == 0;
    }
    bool equals(int p, lcdf::Str s) const {
	return equals(p, s.s, s.len);
    }

    int compare(int p, const char *s, int len) const {
	info_type info = info_[p];
	int minlen = std::min(len, info_len(info));
	int cmp = memcmp(s_ + info_pos(info), s, minlen);
	return cmp ? cmp : ::compare(info_len(info), len);
    }
    int compare(int p, lcdf::Str s) const {
	return compare(p, s.s, s.len);
    }

    bool assign(int p, const char *s, int len) {
	int pos, mylen = info_len(info_[p]);
	if (mylen >= len)
	    pos = info_pos(info_[p]);
	else if (size() + len <= allocated_size()) {
	    pos = size();
	    main_ = make_info(pos + len, allocated_size());
	} else
	    return false;
	memcpy(s_ + pos, s, len);
	info_[p] = make_info(pos, len);
	return true;
    }
    bool assign(int p, lcdf::Str s) {
	return assign(p, s.s, s.len);
    }

    void print(int width, FILE *f, const char *prefix, int indent) {
	fprintf(f, "%s%*s%p (%d:)%d:%d [%d]...\n", prefix, indent, "",
		this, (int) overhead(width), size(), allocated_size(), max_halfinfo + 1);
	for (int i = 0; i < width; ++i)
	    if (info_len(info_[i]))
		fprintf(f, "%s%*s  #%x %d:%d %.*s\n", prefix, indent, "",
			i, info_pos(info_[i]), info_len(info_[i]), std::min(info_len(info_[i]), 40), s_ + info_pos(info_[i]));
    }

  private:

    union {
	struct {
	    info_type main_;
	    info_type info_[0];
	};
	char s_[0];
    };

    static info_type make_info(int pos, int len) {
	return (info_type(pos) << (4 * sizeof(info_type))) | len;
    }
    static int info_pos(info_type info) {
	return info >> (4 * sizeof(info));
    }
    static int info_len(info_type info) {
	return info & ((info_type(1) << (4 * sizeof(info))) - 1);
    }

};

#endif
