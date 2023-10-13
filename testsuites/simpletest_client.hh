#ifndef SIMPLETEST_CLIENT_HH
#define SIMPLETEST_CLIENT_HH

#include "../json.hh"
#include "../kvrandom.hh"
#include "../kvthread.hh"
#include "../nodeversion.hh"
#include "../str.hh"
#include "../string.hh"
#include "../misc.hh"
#include "simpletest_config.hh"

using lcdf::Json;
using lcdf::Str;
using lcdf::String;

template <typename T, typename Derived> class simple_kvtest_client {
public:
  using table_type = T;

  simple_kvtest_client() : limit_(test_limit), ncores_(udpthreads) {}

  int nthreads() const { return udpthreads; }

  int id() const { return ti_->index(); }

  void set_table(T *table, threadinfo *ti) {
    table_ = table;
    ti_ = ti;
  }
  void reset(const String &test, int trial) {
    report_ = Json()
                  .set("table", T().name())
                  .set("test", test)
                  .set("trial", trial)
                  .set("thread", ti_->index());
  }

  bool timeout(int which) const { return ::timeout[which]; }

  uint64_t limit() const { return limit_; }

  bool has_param(const String &name) const { return test_param.count(name); }

  Json param(const String &name, Json default_value = Json()) const {
    return test_param.count(name) ? test_param.at(name) : default_value;
  }

  int ncores() const { return ncores_; }

  double now() const { return ::now(); }

  int ruscale_partsz() const { return nseqkeys() / udpthreads; }

  int ruscale_init_part_no() const { return ti_->index(); }

  long nseqkeys() const { return 140 * 1000000; }

  void get_check(Str key, Str expected)  {
    return static_cast<Derived *>(this)->get_check(key, expected);
  }

  void get_check_absent(Str key) {
    return static_cast<Derived *>(this)->get_check_absent(key);

  }

  void put(Str key, Str value) {
    return static_cast<Derived *>(this)->put(key, value);
  }

  void insert_check(Str key, Str value) {
    return static_cast<Derived *>(this)->insert_check(key, value);
  }

  void get_check(const char *key, const char *expected) {
    get_check(Str(key), Str(expected));
  }

  void get_check(long ikey, long iexpected) {
    quick_istr key(ikey), expected(iexpected);
    get_check(key.string(), expected.string());
  }

  void get_check(Str key, long iexpected) {
    quick_istr expected(iexpected);
    get_check(key, expected.string());
  }

  void put(const char *key, const char *value) {
    put(Str(key), Str(value));
  }
  void put(long ikey, long ivalue) {
    quick_istr key(ikey), value(ivalue);
    put(key.string(), value.string());
  }
  void put(Str key, long ivalue) {
    quick_istr value(ivalue);
    put(key, value.string());
  }

  void print() {
    return static_cast<Derived *>(this)->print();
  }

  void puts_done() {
    return static_cast<Derived *>(this)->puts_done();
  }

  void wait_all() {
    return static_cast<Derived *>(this)->wait_all();
  }

  void rcu_quiesce() {
    mrcu_epoch_type e = timestamp() >> 16;
    if (e != globalepoch)
      set_global_epoch(e);
    ti_->rcu_quiesce();
  }

  String make_message(lcdf::StringAccum &sa) const;
  void notice(const char *fmt, ...);
  void fail(const char *fmt, ...);
  const Json &report(const Json &x) { return report_.merge(x); }
  void finish() {
    Json counters;
    for (int i = 0; i < tc_max; ++i) {
      if (uint64_t c = ti_->counter(threadcounter(i)))
        counters.set(threadcounter_names[i], c);
    }
    if (counters) {
      report_.set("counters", counters);
    }
    if (!quiet) {
      fprintf(stderr, "%d: %s\n", ti_->index(), report_.unparse().c_str());
    }
  }

  static const char* tname() { return typeid(Derived).name(); }

  T *table_;
  threadinfo *ti_;
  kvrandom_lcg_nr rand;
  uint64_t limit_;
  int ncores_;
  Json req_;
  Json report_;
};

template <typename T, typename C>
String simple_kvtest_client<T, C>::make_message(lcdf::StringAccum &sa) const {
  const char *begin = sa.begin();
  while (begin != sa.end() && isspace((unsigned char)*begin))
    ++begin;
  String s = String(begin, sa.end());
  if (!s.empty() && s.back() != '\n')
    s += '\n';
  return s;
}

template <typename T, typename C>
void simple_kvtest_client<T, C>::notice(const char *fmt, ...) {
  va_list val;
  va_start(val, fmt);
  String m = make_message(lcdf::StringAccum().vsnprintf(500, fmt, val));
  va_end(val);
  if (m && !quiet)
    fprintf(stderr, "%d: %s", ti_->index(), m.c_str());
}

template <typename T> inline void kvtest_print(const T &table, FILE* f, threadinfo *ti) {
    // only print out the tree from the first failure
    while (!bool_cmpxchg((int *) &kvtest_printing, 0, ti->index() + 1)) {
    }
    table.print(f);
}

template <typename T, typename C>
void simple_kvtest_client<T, C>::fail(const char *fmt, ...) {
  static nodeversion32 failing_lock(false);
  static nodeversion32 fail_message_lock(false);
  static String fail_message;

  va_list val;
  va_start(val, fmt);
  String m = make_message(lcdf::StringAccum().vsnprintf(500, fmt, val));
  va_end(val);
  if (!m)
    m = "unknown failure";

  fail_message_lock.lock();
  if (fail_message != m) {
    fail_message = m;
    fprintf(stderr, "%d: %s", ti_->index(), m.c_str());
  }
  fail_message_lock.unlock();

  failing_lock.lock();
  fprintf(stdout, "%d: %s", ti_->index(), m.c_str());
  kvtest_print(*table_, stdout, ti_);

  always_assert(0);
}
#endif