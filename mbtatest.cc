#include "nodeversion.hh"
#include "query_masstree.hh"
#include "kvthread.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"

#include "MassTrans.hh"

kvepoch_t global_log_epoch = 0;
volatile uint64_t globalepoch = 1;     // global epoch, updated by main thread regularly
kvtimestamp_t initial_timestamp;
volatile bool recovering = false; // so don't add log entries, and free old value immediately

int main() {
  Masstree::MassTrans<std::string> tree;
  unsigned char buzz[64] = "buzz";
  char foo[64] =  "foo";
  unsigned char *b;
  std::string s = "buzz";
  std::string s2;
  tree.put(Masstree::Str(foo), s);
  assert(tree.get(Masstree::Str(foo), s2));
  s[0] = 'f';
  printf("%s\n", s2.data());
#if 1
  Transaction t;
  tree.transUpdate(t, Masstree::Str(foo), s);
  assert(tree.transGet(t, Masstree::Str(foo), s2));
  assert(t.commit());
  printf("%s\n", s2.data());
#endif

  assert(tree.get(Masstree::Str(foo), s2));
  printf("%s\n", s2.data());

}
