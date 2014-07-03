#pragma once

#include "masstree.hh"
#include "kvthread.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_print.hh"
#include "masstree_remove.hh"
#include "string.hh"
#include "sto/Transaction.hh"

#if 1
class debug_threadinfo {
public:
#if 0
  debug_threadinfo()
    : ts_(0) { // XXX?
  }
#endif

  class rcu_callback {
  public:
    virtual void operator()(debug_threadinfo& ti) = 0;
  };

private:
  static inline void rcu_callback_function(void* p) {
    debug_threadinfo ti;
    static_cast<rcu_callback*>(p)->operator()(ti);
  }


public:
  // XXX Correct node timstamps are needed for recovery, but for no other
  // reason.
  kvtimestamp_t operation_timestamp() const {
    return 0;
  }
  kvtimestamp_t update_timestamp() const {
    return ts_;
  }
  kvtimestamp_t update_timestamp(kvtimestamp_t x) const {
    if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
      // x might be a marker timestamp; ensure result is not
      ts_ = (x | 1) + 1;
    return ts_;
  }
  kvtimestamp_t update_timestamp(kvtimestamp_t x, kvtimestamp_t y) const {
    if (circular_int<kvtimestamp_t>::less(x, y))
      x = y;
    if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
      // x might be a marker timestamp; ensure result is not
      ts_ = (x | 1) + 1;
    return ts_;
  }
  void increment_timestamp() {
    ts_ += 2;
  }
  void advance_timestamp(kvtimestamp_t x) {
    if (circular_int<kvtimestamp_t>::less(ts_, x))
      ts_ = x;
  }

  // event counters
  void mark(threadcounter) {
  }
  void mark(threadcounter, int64_t) {
  }
  bool has_counter(threadcounter) const {
    return false;
  }
  uint64_t counter(threadcounter ci) const {
    return 0;
  }

  /** @brief Return a function object that calls mark(ci); relax_fence().
   *
   * This function object can be used to count the number of relax_fence()s
   * executed. */
  relax_fence_function accounting_relax_fence(threadcounter) {
    return relax_fence_function();
  }

  class accounting_relax_fence_function {
  public:
    template <typename V>
    void operator()(V) {
      relax_fence();
    }
  };
  /** @brief Return a function object that calls mark(ci); relax_fence().
   *
   * This function object can be used to count the number of relax_fence()s
   * executed. */
  accounting_relax_fence_function stable_fence() {
    return accounting_relax_fence_function();
  }

  relax_fence_function lock_fence(threadcounter) {
    return relax_fence_function();
  }

  void* allocate(size_t sz, memtag) {
    return malloc(sz);
  }
  void deallocate(void* p, size_t sz, memtag) {
    // in C++ allocators, 'p' must be nonnull
    free(p);
  }
  void deallocate_rcu(void *p, size_t sz, memtag) {
  }

  void* pool_allocate(size_t sz, memtag) {
    int nl = (sz + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
    return malloc(nl * CACHE_LINE_SIZE);
  }
  void pool_deallocate(void* p, size_t sz, memtag) {
  }
  void pool_deallocate_rcu(void* p, size_t sz, memtag) {
  }

  // RCU
  void rcu_register(rcu_callback *cb) {
    //    scoped_rcu_base<false> guard;
    //rcu::s_instance.free_with_fn(cb, rcu_callback_function);
  }

private:
  mutable kvtimestamp_t ts_;
};


#endif

template <typename V>
class MassTrans : public Shared {
public:
  typedef V value_type;
  typedef debug_threadinfo threadinfo;
  typedef Masstree::Str Str;

private:
  typedef uint32_t Version;
  struct versioned_value {
    Version version;
    value_type value;

    void print(FILE* f, const char* prefix,
               int indent, Str key, kvtimestamp_t initial_timestamp,
               char* suffix) {
      fprintf(f, "%s%*s%.*s = %d%s (version %d)\n", prefix, indent, "", key.len, key.s, value, suffix, version);
    }
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
    if (found) {
      versioned_value *e = lp.value();
      auto& item = t.item(this, e);
      if (!validityCheck(item, e)) {
        t.abort();
        return false;
      }
      if (item.has_write()) {
        // read directly from the element if we're inserting it
        retval = has_insert(item) ? e->value : item.template write_value<value_type>();
        return true;
      }
      Version elem_vers;
      atomicRead(e, elem_vers, retval);
      if (!item.has_read()) {
        t.add_read(item, elem_vers);
      }
    } else {
      ensureNotFound(t, lp.node(), lp.full_version_value());
    }
    return found;
  }

  template <bool INSERT = true, bool SET = true>
  bool transPut(Transaction& t, Str key, const value_type& value, threadinfo& ti = mythreadinfo) {
    cursor_type lp(table_, key);
    bool found = lp.find_insert(ti);
    if (found) {
      versioned_value *e = lp.value();
      lp.finish(0, ti);
      auto& item = t.item(this, e);
      if (!validityCheck(item, e)) {
        t.abort();
        return false;
      }
      if (SET) {
        // if we're inserting this element already we can just update the value we're inserting
        if (has_insert(item))
          e->value = value;
        else
          t.add_write(item, value);
      }
      return found;
    } else {
      if (!INSERT) {
        // TODO: previous_full_version_value is not correct here
        assert(0);
        ensureNotFound(t, lp.node(), lp.previous_full_version_value());
        lp.finish(0, ti);
        return found;
      }

      auto val = new versioned_value;
      val->value = value;
      val->version = invalid_bit;
      fence();
      lp.value() = val;
      lp.finish(1, ti);
      fence();

      auto old_node = lp.old_node();
      auto old_version = lp.old_version_value();
      auto new_version = lp.new_version_value();

      auto node_item = t.has_item(this, tag_inter(old_node));
      if (node_item) {
        if (node_item->has_read() && 
            old_version == node_item->template read_value<typename unlocked_cursor_type::nodeversion_value_type>()) {
          t.add_read(*node_item, new_version);
          // add any new nodes as a result of splits, etc. to the read/absent set
          for (auto&& pair : lp.newnodes()) {
            t.add_read(t.add_item<false>(this, tag_inter(pair.first)), pair.second);
          }
        } else {
          //printf("couldn't find old version: %u vs %u\n", old_version, node_item->template read_value<typename unlocked_cursor_type::nodeversion_value_type>());
          //auto& item = t.add_item<false>(this, val);
          //t.add_write(item, key);
          //t.add_undo(item);
          //t.abort();
          //return false;
        }
      }// else printf("couldn't find node\n");
      auto& item = t.add_item<false>(this, val);
      // TODO: this isn't great because it's going to require an extra alloc (because Str/std::string is 2 words)...
      // we convert to std::string because Str objects are not copied!!
      t.add_write(item, std::string(key));
      t.add_undo(item);
      return found;
    }
  }

  bool transUpdate(Transaction& t, Str k, const value_type& v, threadinfo& ti = mythreadinfo) {
    return transPut</*insert*/false, /*set*/true>(t, k, v, ti);
  }

  bool transInsert(Transaction& t, Str k, const value_type& v, threadinfo&ti = mythreadinfo) {
    return !transPut</*insert*/true, /*set*/false>(t, k, v, ti);
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
    if (is_inter(item.key())) {
      auto n = untag_inter(unpack<leaf_type*>(item.key()));
      auto cur_version = n->full_version_value();
      auto read_version = item.template read_value<typename unlocked_cursor_type::nodeversion_value_type>();
      //      if (cur_version != read_version)
      //printf("node versions disagree: %d vs %d\n", cur_version, read_version);
      return cur_version == read_version;
        //&& !(cur_version & (unlocked_cursor_type::nodeversion_type::traits_type::lock_bit));
      
    }
    auto e = unpack<versioned_value*>(item.key());
    auto read_version = item.template read_value<Version>();
    //    if (read_version != e->version)
    //printf("leaf versions disagree: %d vs %d\n", e->version, read_version);
    return validityCheck(item, e) && versionCheck(read_version, e->version) && (isReadWrite || !is_locked(e->version));
  }
  void install(TransItem& item) {
    assert(!is_inter(item.key()));
    auto e = unpack<versioned_value*>(item.key());
    assert(is_locked(e->version));
    if (!has_insert(item))
      e->value = item.template write_value<value_type>();
    // also marks valid if needed
    inc_version(e->version);
  }

  void undo(TransItem& item) {
    // remove node
    auto& stdstr = item.template write_value<std::string>();
    Str s(stdstr);
    bool success = remove(s);
    assert(success);
  }

  void cleanup(TransItem& item) {
    free_packed<versioned_value*>(item.key());
    if (item.has_read())
      free_packed<Version>(item.data.rdata);
    if (item.has_write())
      free_packed<value_type>(item.data.wdata);
  }

  bool remove(const Str& key) {
    auto ti = mythreadinfo;
    cursor_type lp(table_, key);
    bool found = lp.find_locked(ti);
    lp.finish(found ? -1 : 0, ti);
    // rcu the value
    return found;
  }

#endif

  void print() {
    table_.print();
  }
  

  void transWrite(Transaction& t, int k, value_type v) {
    char s[16];
    sprintf(s, "%d", k);
    transPut(t, s, v);
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
  template <typename NODE, typename VERSION>
  void ensureNotFound(Transaction& t, NODE n, VERSION v) {
    auto& item = t.item(this, tag_inter(n));
    if (!item.has_read()) {
      t.add_read(item, v);
    }
  }

  bool has_insert(TransItem& item) {
    return item.has_undo();
  }

  bool validityCheck(TransItem& item, versioned_value *e) {
    return has_insert(item) || !(e->version & invalid_bit);
  }

  static constexpr Version lock_bit = 1U<<(sizeof(Version)*8 - 1);
  static constexpr Version invalid_bit = 1U<<(sizeof(Version)*8 - 2);
  static constexpr Version version_mask = ~(lock_bit|invalid_bit);

  static constexpr uintptr_t internode_bit = 1<<0;

  template <typename T>
  T* tag_inter(T* p) {
    return (T*)((uintptr_t)p | internode_bit);
  }

  template <typename T>
  T* untag_inter(T* p) {
    return (T*)((uintptr_t)p & ~internode_bit);
  }
  template <typename T>
  bool is_inter(T* p) {
    return (uintptr_t)p & internode_bit;
  }

  bool versionCheck(Version v1, Version v2) {
    return ((v1 ^ v2) & version_mask) == 0;
  }
  void inc_version(Version& v) {
    assert(is_locked(v));
    Version cur = v & version_mask;
    cur = (cur+1) & version_mask;
    // set new version and ensure invalid bit is off
    v = (cur | (v & ~version_mask)) & ~invalid_bit;
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

  struct table_params : public Masstree::nodeparams<15,15> {
    typedef versioned_value* value_type;
    typedef Masstree::value_print<value_type> value_print_type;
    typedef debug_threadinfo threadinfo_type;
  };
  typedef Masstree::basic_table<table_params> table_type;
  typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
  typedef Masstree::tcursor<table_params> cursor_type;
  typedef Masstree::leaf<table_params> leaf_type;
  table_type table_;
};

  template <typename V>
  __thread typename MassTrans<V>::threadinfo MassTrans<V>::mythreadinfo;
