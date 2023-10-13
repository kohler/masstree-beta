#include "binarytree.hh"
#include "compiler.hh"
#include "kvrandom.hh"
#include "kvthread.hh"
#include "misc.hh"
#include "str.hh"
#include <cstdio>
#include <iostream>
#include <string>

volatile uint64_t globalepoch =
    1; // global epoch, updated by main thread regularly
volatile uint64_t active_epoch = 1;
using lcdf::Str;
using mytree = binarytree::binary_tree<binarytree::tree_params<16>>;

inline void put(mytree* bt, threadinfo* ti, binarytree::query& q, long k, long v) {
  quick_istr key(k), value(v);
  q.put(*bt, key.string(), value.string(), *ti);
}


int main() {

  kvrandom_lcg_nr rand;

  rand.seed(31949+2);
  threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  main_ti->pthread() = pthread_self();

  mytree *bt = new mytree();
  bt->initialize(*main_ti);

  binarytree::query q;
  auto v = q.get(*bt, "hello");
  assert(v == nullptr);
  std::cout << "v1: "
            << "EMPTY" << std::endl;
  q.put(*bt, Str("hello"), Str("world"), *main_ti);
  auto v2 = q.get(*bt, "hello");
  std::cout << "v2: " << v2->data() << std::endl;

  for(int i = 1000002; i>1000000; i--) {
    put(bt, main_ti, q, i, i+1);
    std::cout << "getValue: " << i << ":" << q.get(*bt, quick_istr(i).string())->data() << std::endl;
  }
  std::cout << "getValue: " << 1000002 << ":" << q.get(*bt, quick_istr(1000002).string())->data() << std::endl;

  for (int i = 0; i < 3; i++) {
    long x = rand();
    put(bt, main_ti, q, x, x + 1);
  }

  put(bt, main_ti, q, 10, 20);
  q.put(*bt, Str("hellool"), Str("worldddd"), *main_ti);
  q.put(*bt, Str("hellll"), quick_istr(666).string(), *main_ti);
  q.put(*bt, quick_istr(777).string(), quick_istr(444).string(), *main_ti);
  bt->print(stdout);
}