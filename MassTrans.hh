#pragma once

#include "masstree.hh"
#include "kvthread.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "string.hh"
#include "sto/Transaction.hh"

namespace Masstree {

template <typename V>
class MassTrans : public Shared {
public:
  typedef V value_type;

private:
  typedef uint32_t Version;
  struct versioned_value {
    Version version;
    value_type value;
  };

public:

  MassTrans() { table_.initialize(mythreadinfo); }

  static __thread threadinfo mythreadinfo;

  bool put(Str key, value_type value, threadinfo& ti = mythreadinfo) {
    cursor_type lp(table_, key);
    bool found = lp.find_insert(ti);
    if (found) {
      // deallocate_rcu value
      lock(&lp.value()->version);
    } else {
      lp.value() = new versioned_value;
    }
    lp.value()->value = value;
    if (found) {
      inc_version(lp.value()->version);
      unlock(&lp.value()->version);
    } else {
      lp.value()->version = 0;
    }
    lp.finish(1, ti);
    return found;
  }

  bool get(Str key, value_type& value, threadinfo& ti = mythreadinfo) {
    unlocked_cursor_type lp(table_, key);
    bool found = lp.find_unlocked(ti);
    if (found)
      value = lp.value()->value;
    return found;
  }

#if 1
  bool transGet(Transaction& t, Str key, value_type& retval, threadinfo& ti = mythreadinfo) {
    unlocked_cursor_type lp(table_, key);
    bool found = lp.find_unlocked(ti);
    assert(found);
    versioned_value *e = lp.value();
    auto& item = t.item(this, e);
    if (item.has_write()) {
      retval = item.template write_value<value_type>();
      return true;
    }
    Version elem_vers;
    atomicRead(e, elem_vers, retval);
    if (!item.has_read()) {
      t.add_read(item, elem_vers);
    }
    return found;
  }

  bool transUpdate(Transaction& t, Str key, value_type value, threadinfo& ti = mythreadinfo) {
    cursor_type lp(table_, key);
    bool found = lp.find_insert(ti);
    if (!found)
      return false;
    versioned_value *e = lp.value();
    lp.finish(0, ti);
    auto& item = t.item(this, e);
    t.add_write(item, value);
    return found;
  }

  void lock(versioned_value *e) {
    lock(&e->version);
  }
  void unlock(versioned_value *e) {
    unlock(&e->version);
  }

  void lock(TransItem& item) {
    lock(unpack<versioned_value*>(item.key()));
  }
  void unlock(TransItem& item) {
    unlock(unpack<versioned_value*>(item.key()));
  }
  bool check(TransItem& item, bool isReadWrite) {
    auto e = unpack<versioned_value*>(item.key());
    auto read_version = item.template read_value<Version>();
    return versionCheck(read_version, e->version) && (isReadWrite || !is_locked(e->version));
  }
  void install(TransItem& item) {
    auto e = unpack<versioned_value*>(item.key());
    assert(is_locked(e->version));
    e->value = item.template write_value<value_type>();
    inc_version(e->version);
  }

  void cleanup(TransItem& item) {
    free_packed<internal_elem*>(item.key());
    if (item.has_read())
      free_packed<Version>(item.data.rdata);
    if (item.has_write())
      free_packed<Value>(item.data.wdata);
  }

#endif

  void transWrite(Transaction& t, int k, value_type v) {
    char s[16];
    sprintf(s, "%d", k);
    transUpdate(t, s, v);
  }
  value_type transRead(Transaction& t, int k) {
    char s[16];
    sprintf(s, "%d", k);
    value_type v;
    if (!transGet(t, s, v)) {
      return 0;
    }
    return v;
  }
  value_type transRead_nocheck(Transaction& t, int k) { return value_type(); }
  void transWrite_nocheck(Transaction& t, int k, value_type v) {}
  value_type read(int k) {
    Transaction t;
    return transRead(t, k);
  }

  void put(int k, value_type v) {
    char s[16];
    sprintf(s, "%d", k);
    put(s, v);
  }


private:
  static constexpr Version lock_bit = 1U<<(sizeof(Version)*8 - 1);
  static constexpr Version version_mask = ~lock_bit;

  bool versionCheck(Version v1, Version v2) {
    return ((v1 ^ v2) & version_mask) == 0;
  }
  void inc_version(Version& v) {
    assert(is_locked(v));
    Version cur = v & version_mask;
    cur = (cur+1) & version_mask;
    v = cur | (v & ~version_mask);
  }
  bool is_locked(Version v) {
    return v & lock_bit;
  }
  void lock(Version *v) {
    while (1) {
      Version cur = *v;
      if (!(cur&lock_bit) && bool_cmpxchg(v, cur, cur|lock_bit)) {
        break;
      }
      relax_fence();
    }
  }
  void unlock(Version *v) {
    assert(is_locked(*v));
    Version cur = *v;
    cur &= ~lock_bit;
    *v = cur;
  }

  void atomicRead(versioned_value *e, Version& vers, value_type& val) {
    Version v2;
    do {
      vers = e->version;
      fence();
      val = e->value;
      fence();
      v2 = e->version;
    } while (vers != v2);
  }

  struct table_params : public nodeparams<15,15> {
    typedef versioned_value* value_type;
    typedef value_print<MassTrans::value_type> value_print_type;
    typedef threadinfo threadinfo_type;
  };
  typedef basic_table<table_params> table_type;
  typedef unlocked_tcursor<table_params> unlocked_cursor_type;
  typedef tcursor<table_params> cursor_type;
  table_type table_;
};

  template <typename V>
  __thread threadinfo MassTrans<V>::mythreadinfo;

}
