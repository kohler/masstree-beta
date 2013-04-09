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
#ifndef MASSTREE_PRINT_HH
#define MASSTREE_PRINT_HH
#include "masstree_struct.hh"
#include <stdio.h>
class threadinfo;

namespace Masstree {

template <typename P>
void node_base<P>::print(FILE *f, const char *prefix, int indent, int kdepth)
{
    if (this->isleaf())
	((leaf<P> *) this)->print(f, prefix, indent, kdepth);
    else
	((internode<P> *) this)->print(f, prefix, indent, kdepth);
}

template <typename P>
void leaf<P>::print(FILE *f, const char *prefix, int indent, int kdepth)
{
    f = f ? f : stderr;
    prefix = prefix ? prefix : "";
    typename node_base<P>::nodeversion_type v;
    permuter_type perm;
    do {
	v = *this;
	fence();
	perm = permutation_;
    } while (this->has_changed(v));

    char keybuf[MaxKeyLen];
    fprintf(f, "%s%*sleaf %p: %d keys, version %x, permutation %s, ",
	    prefix, indent, "", this, perm.size(), v.version_value(),
	    perm.unparse().c_str());
    if (nremoved_)
	fprintf(f, "removed %d, ", nremoved_);
    fprintf(f, "parent %p, prev %p, next %p ", parent_, prev_, next_.ptr);
    if (ksuf_ && extrasize64_ < -1)
	fprintf(f, "[ksuf i%dx%d] ", -extrasize64_ - 1, ksuf_->allocated_size() / 64);
    else if (ksuf_)
	fprintf(f, "[ksuf x%d] ", ksuf_->allocated_size() / 64);
    else if (extrasize64_)
	fprintf(f, "[ksuf i%d] ", extrasize64_);
    if (P::debug_level > 0) {
	kvtimestamp_t cts = timestamp_sub(created_at_[0], initial_timestamp);
	fprintf(f, "@" PRIKVTSPARTS, KVTS_HIGHPART(cts), KVTS_LOWPART(cts));
    }
    fputc('\n', f);

    if (v.deleted() || (perm[0] != 0 && prev_))
	fprintf(f, "%s%*s%s = [] #0\n", prefix, indent + 2, "", key_type(ikey_bound()).unparse().c_str());

    char xbuf[15];
    for (int idx = 0; idx < perm.size(); ++idx) {
	int p = perm[idx];
	int l = this->get_key(p).unparse(keybuf, sizeof(keybuf));
	sprintf(xbuf, " #%x/%d", p, keylenx_[p]);
	leafvalue_type lv = lv_[p];
	if (this->has_changed(v)) {
	    fprintf(f, "%s%*s[NODE CHANGED]\n", prefix, indent + 2, "");
	    break;
	} else if (!lv)
	    fprintf(f, "%s%*s%.*s = []%s\n", prefix, indent + 2, "", l, keybuf, xbuf);
	else if (is_node(p)) {
	    fprintf(f, "%s%*s%.*s = SUBTREE%s\n", prefix, indent + 2, "", l, keybuf, xbuf);
	    node_base<P> *n = lv.node()->unsplit_ancestor();
	    n->print(f, prefix, indent + 4, kdepth + key_type::ikey_size);
	} else {
	    typename P::value_type tvx = lv.value();
            tvx->print(f, prefix, indent + 2, Str(keybuf, l), initial_timestamp, xbuf);
	}
    }

    if (v.deleted())
	fprintf(f, "%s%*s[DELETED]\n", prefix, indent + 2, "");
}

template <typename P>
void internode<P>::print(FILE *f, const char *prefix, int indent, int kdepth)
{
    f = f ? f : stderr;
    prefix = prefix ? prefix : "";
    internode<P> copy(*this);
    for (int i = 0; i < 100 && (copy.has_changed(*this) || this->inserting() || this->splitting()); ++i)
	memcpy(&copy, this, sizeof(copy));

    char keybuf[MaxKeyLen];
    fprintf(f, "%s%*sinternode %p%s: %d keys, version %x, parent %p",
	    prefix, indent, "", this, this->deleted() ? " [DELETED]" : "",
	    copy.size(), copy.version_value(), copy.parent_);
    if (P::debug_level > 0) {
	kvtimestamp_t cts = timestamp_sub(created_at_[0], initial_timestamp);
	fprintf(f, " @" PRIKVTSPARTS, KVTS_HIGHPART(cts), KVTS_LOWPART(cts));
    }
    fputc('\n', f);
    for (int p = 0; p < copy.size(); ++p) {
	if (copy.child_[p])
	    copy.child_[p]->print(f, prefix, indent + 4, kdepth);
	else
	    fprintf(f, "%s%*s[]\n", prefix, indent + 4, "");
	int l = copy.get_key(p).unparse(keybuf, sizeof(keybuf));
	fprintf(f, "%s%*s%.*s\n", prefix, indent + 2, "", l, keybuf);
    }
    if (copy.child_[copy.size()])
	copy.child_[copy.size()]->print(f, prefix, indent + 4, kdepth);
    else
	fprintf(f, "%s%*s[]\n", prefix, indent + 4, "");
}

template <typename P>
void basic_table<P>::print(FILE *f, int indent) const {
    root_->print(f ? f : stdout, "", indent, 0);
}

} // namespace Masstree
#endif
