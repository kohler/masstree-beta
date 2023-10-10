#ifndef SIMPLETEST_BINARYTREE_HH
#define SIMPLETEST_BINARYTREE_HH

#include "../kvtest.hh"
#include "../binarytree.hh"
#include "simpletest_client.hh"
#include "simpletest_runner.hh"

class binarytree_test_client
    : public simple_kvtest_client<binarytree::default_table, binarytree_test_client> {
public:
  using simple_kvtest_client<binarytree::default_table, binarytree_test_client>::simple_kvtest_client;

  void get_check(Str key, Str expected);
  void get_check_absent(Str key);
  void put(Str key, Str value);
  void insert_check(Str key, Str value);
  void print() {}

  void puts_done() {}
  void wait_all() {}

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

private:
  binarytree::query q_[1];
};

MAKE_TESTRUNNER(binarytree_test_client, simple, kvtest_simple(client));
MAKE_TESTRUNNER(binarytree_test_client, rw1, kvtest_rw1(client));
MAKE_TESTRUNNER(binarytree_test_client, rw1puts, kvtest_rw1puts(client));
MAKE_TESTRUNNER(binarytree_test_client, ruscale_init,
                kvtest_ruscale_init(client));
MAKE_TESTRUNNER(binarytree_test_client, rscale, kvtest_rscale(client));
MAKE_TESTRUNNER(binarytree_test_client, uscale, kvtest_uscale(client));

void binarytree_test_client::get_check(Str key, Str expected) {
  auto v = q_[0].get(*table_, key);
  if (unlikely(v == nullptr)) {
    fail("get(%s) failed (expected %s)\n", String(key).printable().c_str(),
         String(expected).printable().c_str());
  } else if (unlikely(expected != *v)) {
    fail("get(%s) returned unexpected value %s (expected %s)\n",
         String(key).printable().c_str(),
         String(*v).substr(0, 40).printable().c_str(),
         String(expected).substr(0, 40).printable().c_str());
  }
}

void binarytree_test_client::get_check_absent(Str key) {
  auto v = q_[0].get(*table_, key);
  if (unlikely(v != nullptr)) {
    fail("get(%s) failed (expected absent key)\n",
         String(key).printable().c_str());
  }
}

void binarytree_test_client::put(Str key, Str value) {
  q_[0].put(*table_, key, value, *ti_);
}

void binarytree_test_client::insert_check(Str key, Str value) {
}

#endif