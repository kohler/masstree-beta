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
#include "masstree.hh"
#include "masstree_key.hh"
#include "masstree_struct.hh"
#include "masstree_tcursor.hh"
#include "masstree_traverse.hh"
#include "masstree_get.hh"
#include "masstree_insert.hh"
#include "masstree_split.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "masstree_print.hh"
#include "masstree_query.hh"
#include "string_slice.hh"
#include "kpermuter.hh"
#include "ksearch.hh"
#include "stringbag.hh"
#include "json.hh"
#include "kvrow.hh"

namespace Masstree {

/** @brief Assign position @a p's keysuffix to @a s.

    This version of assign_ksuf() is be called when @a s might not fit into
    the current keysuffix container. It may allocate a new container, copying
    suffixes over.

    The @a initializing parameter determines which suffixes are copied. If @a
    initializing is false, then this is an insertion into a live node. The
    live node's permutation indicates which keysuffixes are active, and only
    active suffixes are copied. If @a initializing is true, then this
    assignment is part of the initialization process for a new node. The
    permutation might not be set up yet. In this case, it is assumed that key
    positions [0,p) are ready: keysuffixes in that range are copied. In either
    case, the key at position p is NOT copied; it is assigned to @a s. */
template <typename P>
void leaf<P>::hard_assign_ksuf(int p, Str s, bool initializing,
			       threadinfo* ti) {
    if (ksuf_ && ksuf_->assign(p, s))
	return;

    stringbag<uint16_t> *iksuf;
    stringbag<uint32_t> *oksuf;
    if (extrasize64_ > 0)
	iksuf = &iksuf_[0], oksuf = 0;
    else
	iksuf = 0, oksuf = ksuf_;

    size_t csz;
    if (iksuf)
	csz = iksuf->allocated_size() - iksuf->overhead(width);
    else if (oksuf)
	csz = oksuf->allocated_size() - oksuf->overhead(width);
    else
	csz = 0;
    size_t sz = iceil_log2(std::max(csz, size_t(4 * width)) * 2);
    while (sz < csz + stringbag<uint32_t>::overhead(width) + s.len)
	sz *= 2;

    void *ptr = ti->allocate(sz, memtag_masstree_ksuffixes, ta_tree);
    stringbag<uint32_t> *nksuf = new(ptr) stringbag<uint32_t>(width, sz);
    permuter_type perm(permutation_);
    int n = initializing ? p : perm.size();
    for (int i = 0; i < n; ++i) {
	int mp = initializing ? i : perm[i];
	if (mp != p && has_ksuf(mp)) {
	    bool ok = nksuf->assign(mp, ksuf(mp));
            assert(ok);
        }
    }
    bool ok = nksuf->assign(p, s);
    assert(ok);
    fence();

    if (nremoved_ > 0)		// removed ksufs are not copied to the new ksuf,
	this->mark_insert();	// but observers might need removed ksufs:
				// make them retry

    ksuf_ = nksuf;
    fence();

    if (extrasize64_ >= 0)	// now the new ksuf_ installed, mark old dead
	extrasize64_ = -extrasize64_ - 1;

    if (oksuf)
	ti->deallocate_rcu(oksuf, oksuf->allocated_size(),
			   memtag_masstree_ksuffixes, ta_tree, 0);
}


template <typename P>
bool query_table<P>::get(query<row_type>& q, threadinfo* ti) const {
    ti->pstat.mark_get_begin();
    unlocked_tcursor<P> lp(table_, q.key_);
    bool found = lp.find_unlocked(ti);
    if (found)
        found = q.emitrow(lp.datum_, ti);
    ti->pstat.mark_get_end();
    return found;
}

template <typename P>
result_t query_table<P>::put(query<row_type>& q, threadinfo* ti) {
    tcursor<P> lp(table_, q.key_);
    bool found = lp.find_insert(ti);
    if (!found)
	ti->advance_timestamp(lp.node_timestamp());
    result_t r = q.apply_put(lp.value(), found, ti);
    lp.finish(1, ti);
    return r;
}

template <typename P>
void query_table<P>::replace(query<row_type>& q, threadinfo* ti) {
    tcursor<P> lp(table_, q.key_);
    bool found = lp.find_insert(ti);
    if (!found)
	ti->advance_timestamp(lp.node_timestamp());
    q.apply_replace(lp.value(), found, ti);
    lp.finish(1, ti);
}

template <typename P>
void query_table<P>::replay(replay_query<row_type>& q, threadinfo* ti) {
    tcursor<P> lp(table_, q.key_);
    bool found = lp.find_insert(ti);
    if (!found)
	ti->advance_timestamp(lp.node_timestamp());
    q.apply(lp.value(), found, ti);
    lp.finish(1, ti);
}

template <typename P>
void query_table<P>::scan(query<row_type>& q, threadinfo* ti) const {
    query_scanner<row_type> scanf(q);
    table_.scan(q.key_, true, scanf, ti);
}

template <typename P>
void query_table<P>::rscan(query<row_type>& q, threadinfo* ti) const {
    query_scanner<row_type> scanf(q);
    table_.rscan(q.key_, true, scanf, ti);
}

template <typename P>
bool query_table<P>::remove(query<row_type>& q, threadinfo* ti) {
    tcursor<P> lp(table_, q.key_);
    bool found = lp.find_locked(ti);
    bool removed = found
	&& q.apply_remove(lp.value(), true, ti, &lp.node_timestamp());
    lp.finish(removed ? -1 : 0, ti);
    return removed;
}

template <typename P>
void query_table<P>::checkpoint_restore(Str key, Str value, kvtimestamp_t ts,
                                        threadinfo* ti) {
    tcursor<P> lp(table_, key);
    bool found = lp.find_insert(ti);
    invariant(!found);
    ti->advance_timestamp(lp.node_timestamp());
    lp.value() = row_type::checkpoint_read(value, ts, *ti);
    lp.finish(1, ti);
}


static uint64_t heightcounts[300], fillcounts[100];

template <typename P>
static void treestats1(node_base<P>* n, unsigned height) {
    if (!n)
	return;
    if (n->isleaf()) {
	assert(height < arraysize(heightcounts));
        if (n->deleted())
            return;
        leaf<P> *lf = (leaf<P> *)n;
        typename leaf<P>::permuter_type perm = lf->permutation_;
        for (int idx = 0; idx < perm.size(); ++idx) {
	    int p = perm[idx];
    	    typename leaf<P>::leafvalue_type lv = lf->lv_[p];
            if (!lv || !lf->is_node(p))
                heightcounts[height] ++;
 	    else {
                node_base<P> *in = lv.node()->unsplit_ancestor();
                treestats1(in, height + 1);
  	    }
        }
    } else {
	internode<P> *in = (internode<P> *) n;
	for (int i = 0; i <= n->size(); ++i)
	    if (in->child_[i])
		treestats1(in->child_[i], height + 1);
    }
    assert((size_t) n->size() < arraysize(fillcounts));
    fillcounts[n->size()] += 1;
}

template <typename P>
void query_table<P>::stats(FILE* f) {
    memset(heightcounts, 0, sizeof(heightcounts));
    memset(fillcounts, 0, sizeof(fillcounts));
    treestats1(table_.root(), 0);
    fprintf(f, "  heights:");
    for (unsigned i = 0; i < arraysize(heightcounts); ++i)
	if (heightcounts[i])
	    fprintf(f, "  %d=%" PRIu64, i, heightcounts[i]);
    fprintf(f, "\n  node counts:");
    for (unsigned i = 0; i < arraysize(fillcounts); ++i)
	if (fillcounts[i])
	    fprintf(f, "  %d=%" PRIu64, i, fillcounts[i]);
    fprintf(f, "\n");
}

template <typename P>
static void json_stats1(node_base<P> *n, Json &j, int layer, int depth,
			threadinfo *ti)
{
    if (!n)
	return;
    else if (n->isleaf()) {
	leaf<P> *lf = static_cast<leaf<P> *>(n);
	j["l1_node_by_depth" + (!layer * 3)][depth] += 1;
	j["l1_leaf_by_depth" + (!layer * 3)][depth] += 1;
	j["l1_leaf_by_size" + (!layer * 3)][lf->size()] += 1;
	typename leaf<P>::permuter_type perm(lf->permutation_);
	int n = 0;
	for (int i = 0; i < perm.size(); ++i)
	    if (lf->is_node(perm[i])) {
		Json x = j["l1_size"];
		j["l1_size"] = 0;
		json_stats1(lf->lv_[perm[i]].node(), j, layer + 1, 0, ti);
		j["l1_size_sum"] += j["l1_size"].to_i();
		j["l1_size"] = x;
		j["l1_count"] += 1;
	    } else {
		++n;
		int l = sizeof(typename P::ikey_type) * layer
		    + lf->keylenx_[perm[i]];
		if (lf->has_ksuf(perm[i]))
		    l += lf->ksuf(perm[i]).len - 1;
		j["key_by_length"][l] += 1;
	    }
	j["size"] += n;
	j["l1_size"] += n;
	j["key_by_layer"][layer] += n;
    } else {
	internode<P> *in = static_cast<internode<P> *>(n);
	for (int i = 0; i <= n->size(); ++i)
	    if (in->child_[i])
		json_stats1(in->child_[i], j, layer, depth + 1, ti);
	j["l1_node_by_depth" + (!layer * 3)][depth] += 1;
    }
}

template <typename P>
void query_table<P>::json_stats(Json &j, threadinfo *ti)
{
    j["size"] = 0.0;
    j["l1_count"] = 0;
    j["l1_size"] = 0;
    j["node_by_depth"] = Json::make_array();
    j["l1_node_by_depth"] = Json::make_array();
    j["leaf_by_depth"] = Json::make_array();
    j["l1_leaf_by_depth"] = Json::make_array();
    j["leaf_by_size"] = Json::make_array();
    j["l1_leaf_by_size"] = Json::make_array();
    j["key_by_layer"] = Json::make_array();
    j["key_by_length"] = Json::make_array();
    json_stats1(table_.root(), j, 0, 0, ti);
    j.unset("l1_size");
}

template <typename N>
static Str findpv(N *n, int pvi, int npv)
{
    // XXX assumes that most keys differ in the low bytes
    // XXX will fail badly if all keys have the same prefix
    // XXX not clear what to do then
    int nbranch = 1, branchid = 0;
    typedef typename N::internode_type internode_type;
    typedef typename N::leaf_type leaf_type;

    n = n->unsplit_ancestor();

    while (1) {
	typename N::nodeversion_type v = n->stable();
	int size = n->size() + !n->isleaf();
	if (size == 0)
	    return Str();

	int total_nkeys_estimate = nbranch * size;
	int first_pv_in_node = branchid * size;
	int pv_offset = pvi * total_nkeys_estimate / npv - first_pv_in_node;

	if (!n->isleaf() && total_nkeys_estimate < npv) {
	    internode_type *in = static_cast<internode_type *>(n);
	    pv_offset = std::min(std::max(pv_offset, 0), size - 1);
	    N *next = in->child_[pv_offset];
	    if (!n->has_changed(v)) {
		nbranch = total_nkeys_estimate;
		branchid = first_pv_in_node + pv_offset;
		n = next;
	    }
	    continue;
	}

	pv_offset = std::min(std::max(pv_offset, 0), size - 1 - !n->isleaf());
	typename N::ikey_type ikey0;
	if (n->isleaf()) {
	    leaf_type *l = static_cast<leaf_type *>(n);
	    typename leaf_type::permuter_type perm = l->permutation();
	    ikey0 = l->ikey0_[perm[pv_offset]];
	} else {
	    internode_type *in = static_cast<internode_type *>(n);
	    ikey0 = in->ikey0_[pv_offset];
	}

	if (!n->has_changed(v)) {
	    char *x = (char *) malloc(sizeof(ikey0));
	    int len = string_slice<typename N::ikey_type>::unparse_comparable(x, sizeof(ikey0), ikey0);
	    return Str(x, len);
	}
    }
}

// findpivots should allocate memory for pv[i]->s, which will be
// freed by the caller.
template <typename P>
void query_table<P>::findpivots(Str *pv, int npv) const
{
    pv[0].assign(NULL, 0);
    char *cmaxk = (char *)malloc(MaxKeyLen);
    memset(cmaxk, 255, MaxKeyLen);
    pv[npv - 1].assign(cmaxk, MaxKeyLen);
    for (int i = 1; i < npv - 1; i++)
	pv[i] = findpv(table_.root(), i, npv - 1);
}

namespace {
struct scan_tester {
    const char * const *vbegin_, * const *vend_;
    char key_[32];
    int keylen_;
    bool reverse_;
    bool first_;
    scan_tester(const char * const *vbegin, const char * const *vend,
		bool reverse = false)
	: vbegin_(vbegin), vend_(vend), keylen_(0), reverse_(reverse),
	  first_(true) {
	if (reverse_) {
	    memset(key_, 255, sizeof(key_));
	    keylen_ = sizeof(key_);
	}
    }
    bool operator()(Str key, row_type *, threadinfo *) {
	memcpy(key_, key.s, key.len);
	keylen_ = key.len;
	const char *pos = (reverse_ ? vend_[-1] : vbegin_[0]);
	if ((int) strlen(pos) != key.len || memcmp(pos, key.s, key.len) != 0) {
	    fprintf(stderr, "%sscan encountered %.*s, expected %s\n", reverse_ ? "r" : "", key.len, key.s, pos);
	    assert((int) strlen(pos) == key.len && memcmp(pos, key.s, key.len) == 0);
	}
	fprintf(stderr, "%sscan %.*s\n", reverse_ ? "r" : "", key.len, key.s);
	(reverse_ ? --vend_ : ++vbegin_);
	first_ = false;
	return vbegin_ != vend_;
    }
    template <typename T>
    int scan(T &table, threadinfo *ti) {
	return table.table().scan(Str(key_, keylen_), first_, *this, ti);
    }
    template <typename T>
    int rscan(T &table, threadinfo *ti) {
	return table.table().rscan(Str(key_, keylen_), first_, *this, ti);
    }
};
}

template <typename P>
void query_table<P>::test(threadinfo *ti) {
    query_table<P> t;
    t.initialize(ti);
    query<row_type> q;

    const char * const values[] = {
	"", "0", "1", "10", "100000000",			// 0-4
	"1000000001", "1000000002", "2", "20", "200000000",	// 5-9
	"aaaaaaaaaaaaaaaaaaaaaaaaaa",			    	// 10
	"aaaaaaaaaaaaaaabbbb", "aaaaaaaaaaaaaaabbbc", "aaaaaaaaaxaaaaabbbc", "b", "c", "d", "e", "f", "g", "h", "i", "j",
	"kkkkkkkk\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF" "a",
	"kkkkkkkk\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF" "b",
	"xxxxxxxxy"
    };
    const char * const *end_values = values + arraysize(values);
    const char *values_copy[arraysize(values)];
    memcpy(values_copy, values, sizeof(values));

    for (int i = arraysize(values); i > 0; --i) {
	int x = rand() % i;
        q.begin_replace(Str(values_copy[x]), Str(values_copy[x]));
	t.replace(q, ti);
	values_copy[x] = values_copy[i - 1];
    }

    t.table_.print();
    printf("\n");

    scan_tester scanner(values, values + 3);
    while (scanner.scan(t, ti)) {
	scanner.vend_ = std::min(scanner.vend_ + 3, end_values);
	fprintf(stderr, "-scanbreak-\n");
    }

    scanner = scan_tester(values, values + 8);
    while (scanner.scan(t, ti)) {
	scanner.vend_ = std::min(scanner.vend_ + 8, end_values);
	fprintf(stderr, "-scanbreak-\n");
    }

    scanner = scan_tester(values + 10, values + 11);
    int r = t.table_.scan(Str(values[10]), true, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 10, values + 11);
    r = t.table_.scan(Str(values[10] + 1), true, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 11, values + 12);
    r = t.table_.scan(Str(values[10]), false, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 10, values + 11);
    r = t.table_.scan(Str("aaaaaaaaaaaaaaaaaaaaaaaaaZ"), true, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 11, values + 12);
    r = t.table_.scan(Str(values[11]), true, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 12, values + 13);
    r = t.table_.scan(Str(values[11]), false, scanner, ti);
    mandatory_assert(r == 1);


    scanner = scan_tester(end_values - 3, end_values, true);
    while (scanner.rscan(t, ti)) {
	scanner.vbegin_ = std::max(scanner.vbegin_ - 3, (const char * const *) values);
	fprintf(stderr, "-scanbreak-\n");
    }

    scanner = scan_tester(end_values - 2, end_values, true);
    r = scanner.rscan(t, ti);
    mandatory_assert(r == 2);
    scanner.vbegin_ = std::max(scanner.vbegin_ - 2, (const char * const *) values);
    fprintf(stderr, "-scanbreak-\n");
    r = scanner.rscan(t, ti);
    mandatory_assert(r == 2);

    scanner = scan_tester(end_values - 8, end_values, true);
    while (scanner.rscan(t, ti)) {
	scanner.vbegin_ = std::max(scanner.vbegin_ - 8, (const char * const *) values);
	fprintf(stderr, "-scanbreak-\n");
    }

    scanner = scan_tester(values + 10, values + 11);
    r = t.table_.rscan(Str(values[10]), true, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 10, values + 11);
    r = t.table_.rscan(Str("aaaaaaaaaaaaaaaaaaaaaaaaab"), true, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 9, values + 10);
    r = t.table_.rscan(Str(values[10]), false, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 10, values + 11);
    r = t.table_.rscan(Str("aaaaaaaaaaaaaaaaaaaaaaaaab"), true, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 11, values + 12);
    r = t.table_.rscan(Str(values[11]), true, scanner, ti);
    mandatory_assert(r == 1);

    scanner = scan_tester(values + 10, values + 11);
    r = t.table_.rscan(Str(values[11]), false, scanner, ti);
    mandatory_assert(r == 1);


    Str pv[10];
    t.findpivots(pv, 10);
    for (int i = 0; i < 10; ++i) {
	fprintf(stderr, "%d >%.*s<\n", i, std::min(pv[i].len, 10), pv[i].s);
	free((char *)pv[i].s);
    }
    t.findpivots(pv, 4);
    for (int i = 0; i < 4; ++i) {
	fprintf(stderr, "%d >%.*s<\n", i, std::min(pv[i].len, 10), pv[i].s);
	free((char *)pv[i].s);
    }

    // XXX destroy tree
}

template <typename P>
void query_table<P>::print(FILE *f, int indent) const {
    table_.print(f, indent);
}

template class basic_table<default_table::param_type>;
template class query_table<default_table::param_type>;

}
