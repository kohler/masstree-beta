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
#ifndef KVTABLE_HH
#define KVTABLE_HH 1
#include "kvdconfig.hh"
#include "str.hh"
#include "kvproto.hh"
#include <vector>
class threadinfo;
template <class R> struct query;
class Json;

template <typename T> struct table_has_print : public mass::false_type {};
template <typename T> struct table_has_remove : public mass::false_type {};
template <typename T> struct table_has_many_get : public mass::false_type {};
template <typename T> struct table_has_checkpoint : public mass::true_type {};
template <typename T> struct table_has_scan : public mass::true_type {};
template <typename T> struct table_has_rscan : public mass::false_type {};
template <typename T> struct table_has_json_stats : public mass::false_type {};

enum ckptrav_order_t {
    ckptrav_inorder = 0, ckptrav_preorder
};

class kvtable { public:

    kvtable() {
    }

    virtual void initialize(threadinfo *ti) = 0;
    virtual void reinitialize(threadinfo *ti) = 0;
    /** @brief Retrive the value of a key, and emit it if found.
     * @return true if found; false otherwise
     */
    virtual bool get(query<row_type> &q, threadinfo *ti) const = 0;
    /** @brief Insert a value or atomically update a row.
     *    The change is ti->qio.change.
     *  @return OutOfDate if v is out-of-dated; Inserted or Updated otherwise
     */
    virtual result_t put(query<row_type> &q, threadinfo *ti) = 0;
    /** @brief Scan the tree from startkey. Run until q.scanemit says
     *    stop by return false. */
    virtual void scan(query<row_type> &q, threadinfo *ti) const = 0;
    /** @brief Reverse scan the tree from startkey. Run until q.scanemit says
     *    stop by return false. */
    virtual void rscan(query<row_type> &, threadinfo *) const {
    }
    virtual bool remove(query<row_type> &, threadinfo *) {
	return false;
    }
    virtual void many_get(query<row_type> *qs, int nqs, threadinfo *ti) const = 0;
    virtual void findpivots(str *pv, int npv) const = 0;

    virtual void stats(FILE *) = 0;
    virtual void json_stats(Json &j, threadinfo *ti) = 0;
    virtual const char *name() const = 0;

    virtual ckptrav_order_t ckptravorder() const = 0;

    virtual bool has_print() const = 0;
    virtual void print(FILE *, int, threadinfo *) {
    }
    virtual bool has_remove() const = 0;

};


template <bool C> struct kvtable_helper {};
template <> struct kvtable_helper<true> {
    template <typename T> static void many_get(T &table, query<row_type> *qs, int nqs, threadinfo *ti) {
        return table.many_get(qs, nqs, ti);
    }
    template <typename T> static void scan(T &table, query<row_type> &q, threadinfo *ti) {
        table.scan(q, ti);
    }
    template <typename T> static void rscan(T &table, query<row_type> &q, threadinfo *ti) {
        table.rscan(q, ti);
    }
    template <typename T> static bool remove(T &table, query<row_type> &q, threadinfo *ti) {
	return table.remove(q, ti);
    }
    template <typename T> static void print(const T &table, FILE *f, int indent, threadinfo *) {
	table.print(f, indent);
    }
    template <typename T> static void json_stats(T &table, Json &j, threadinfo *ti) {
        return table.json_stats(j, ti);
    }
};
template <> struct kvtable_helper<false> {
    template <typename T> static void many_get(T &, query<row_type> *, int, threadinfo *) {
    }
    template <typename T> static void scan(T &, query<row_type> &, threadinfo *) {
	mandatory_assert(0 && "scan not supported here");
    }
    template <typename T> static void rscan(T &, query<row_type> &, threadinfo *) {
	mandatory_assert(0 && "rscan not supported here");
    }
    template <typename T> static bool remove(T &, query<row_type> &, threadinfo *) {
	return false;
    }
    template <typename T> static void print(const T &, FILE *, int, threadinfo *) {
    }
    template <typename T> static void json_stats(T &, Json &, threadinfo *) {
    }
};

template <bool C> struct kvtable_checkpoint_helper;
template <> struct kvtable_checkpoint_helper<true> {
    template <typename T> static ckptrav_order_t ckptravorder(const T &table) {
	return table.ckptravorder();
    }
    template <typename T> static void findpivots(const T &table, str *pv, int npv) {
        table.findpivots(pv, npv);
    }
};
template <> struct kvtable_checkpoint_helper<false> {
    template <typename T> static ckptrav_order_t ckptravorder(const T &) {
	return ckptrav_preorder;
    }
    template <typename T> static void findpivots(const T &, str *, int) {
	mandatory_assert(0 && "checkpoint not supported here");
    }
};

template <typename T> class kvtable_adapter : public kvtable { public:

    kvtable_adapter() {
    }

    virtual void initialize(threadinfo *ti) {
	t_.initialize(ti);
    }
    virtual void reinitialize(threadinfo *ti) {
	t_.reinitialize(ti);
    }

    virtual bool get(query<row_type> &q, threadinfo *ti) const {
	return t_.get(q, ti);
    }
    virtual void many_get(query<row_type> *qs, int nqs, threadinfo *ti) const {
        kvtable_helper<table_has_many_get<T>::value>::many_get(t_, qs, nqs, ti);
    }
    virtual result_t put(query<row_type> &q, threadinfo *ti) {
	return t_.put(q, ti);
    }
    virtual void scan(query<row_type> &q, threadinfo *ti) const {
	kvtable_helper<table_has_scan<T>::value>::scan(t_, q, ti);
    }
    virtual void rscan(query<row_type> &q, threadinfo *ti) const {
	kvtable_helper<table_has_rscan<T>::value>::rscan(t_, q, ti);
    }

    virtual bool has_remove() const {
	return table_has_remove<T>::value;
    }
    virtual bool remove(query<row_type> &q, threadinfo *ti) {
	return kvtable_helper<table_has_remove<T>::value>::remove(t_, q, ti);
    }

    virtual ckptrav_order_t ckptravorder() const {
	return kvtable_checkpoint_helper<table_has_checkpoint<T>::value>::ckptravorder(t_);
    }
    virtual void findpivots(str *pv, int npv) const {
	kvtable_checkpoint_helper<table_has_checkpoint<T>::value>::findpivots(t_, pv, npv);
    }

    virtual void stats(FILE *f) {
	t_.stats(f);
    }
    virtual void json_stats(Json &j, threadinfo *ti) {
	kvtable_helper<table_has_json_stats<T>::value>::json_stats(t_, j, ti);
    }
    virtual const char *name() const {
	return t_.name();
    }

    virtual bool has_print() const {
	return table_has_print<T>::value;
    }
    virtual void print(FILE *f, int indent, threadinfo *ti) {
	kvtable_helper<table_has_print<T>::value>::print(t_, f, indent, ti);
    }

  private:

    T t_;

};

class kvtable_factory { public:
    kvtable_factory() {
    }
    virtual kvtable *create() = 0;
    virtual bool has_print() const = 0;
    virtual bool has_remove() const = 0;

    static void add(const char *name, kvtable_factory *factory);
    static kvtable_factory *find(const char *name);
    static kvtable *create(const char *name);
    static std::vector<const char *> table_types();
};

template <typename T>
class kvtable_factory_adapter : public kvtable_factory { public:
    kvtable_factory_adapter() {
    }
    virtual kvtable *create() {
	return new kvtable_adapter<T>;
    }
    virtual bool has_print() const {
	return table_has_print<T>::value;
    }
    virtual bool has_remove() const {
	return table_has_remove<T>::value;
    }
};

template <typename T>
class kvtable_registration_adapter : public kvtable_factory_adapter<T> { public:
    kvtable_registration_adapter() {
	kvtable_factory::add(T::name(), this);
    }
    kvtable_registration_adapter(const char *name) {
	kvtable_factory::add(name, this);
    }
    kvtable_registration_adapter(const char *name1, const char *name2) {
	kvtable_factory::add(name1, this);
	kvtable_factory::add(name2, this);
    }
};

#endif
