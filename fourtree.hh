#ifndef FOURTREE_HH
#define FOURTREE_HH
#include "circular_int.hh"
#include "compiler.hh"
#include "fixsizedkey.hh"
#include "kvthread.hh"
#include "mtcounters.hh"
#include "str.hh"
#include "string.hh"
#include <mutex>
#include <stdio.h>
#include <string>

namespace fourtree {
using lcdf::Str;
using lcdf::String;

template <size_t KeySize=16> struct tree_params {
  static constexpr int ikey_size = KeySize;
  static constexpr int fanout = 4;
  using value_type = std::string;
  using threadinfo_type = ::threadinfo;
};

class SpinLock {
private:
  std::atomic_flag flag = ATOMIC_FLAG_INIT;

public:
  void lock() {
    while (flag.test_and_set(std::memory_order_acquire)) {
    }
  }
  void unlock() { flag.clear(std::memory_order_release); }
};

using scoped_lock =  std::lock_guard<SpinLock>;

template <typename P> class node;
template <typename P> class cursor;

template <typename P> class four_tree {
public:
  using parameter_type = P;
  using node_type = node<P>;
  using value_type = typename P::value_type;
  using key_type = fix_sized_key<P::ikey_size>;
  using threadinfo = typename P::threadinfo_type;
  using cursor_type = cursor<P>;

  inline four_tree() {}

  void initialize(threadinfo &ti);
  void destroy(threadinfo &ti);

  inline node_type *root() const { return root_; }

  static const char *name() { return "fourtree"; }

  void print(FILE* f) const {
    print(f, root_);
  }

private:
  node_type *root_;

  void print(FILE* f, node_type *node) const {
    print(f, "", node, false);
  }

  void print(FILE* f,const std::string& prefix, const node_type* node, bool isLeft) const {
      if (node != nullptr) {
        fprintf(f, "%s", prefix.c_str());

        fprintf(f, "%s", (isLeft ? "├──" : "└──"));

        // print the value of the node
        for (int i=0; i<P::fanout-1; i++) {
          if (node->values_[i] == nullptr) {
            fprintf(f, "%d:%s:%s; ", i, "null", "null");
          } else {
            key_type k(node->keys0_[i], node->keys1_[i]);
            fprintf(f, "%d:%s:%s; ", i, k.unparse_printable().c_str(), node->values_[i]->data());
          }
        }
        fprintf(f, "\n");

        // print next level
        for (int i=0; i<P::fanout; i++) {
          print(f, prefix + (isLeft ? "│   " : "    "), node->children_[i], i!=P::fanout-1);
        }
      }
    }
};

template <typename P> class alignas(CACHE_LINE_SIZE) node {
  using key_type = fix_sized_key<P::ikey_size>;
  using value_type = typename P::value_type;
  using threadinfo = typename P::threadinfo_type;
  using node_type = node<P>;

public:
  // first cacheline contains first 8 bytes of keys and 4 children
  SpinLock lk_;
  uint64_t keys1_[P::fanout-1] = {};
  node<P>* children_[P::fanout] = {};
  uint64_t keys0_[P::fanout-1] = {};
  value_type* values_[P::fanout-1] = {};

  node(){}

  static node_type *make(threadinfo &ti) {
    node_type* data = (node_type*) ti.pool_allocate(sizeof(node_type), memtag_masstree_internode);
    new (data) node_type();
    return data;
  }

  int compare_with(const key_type& k, size_t index) const {
    int cmp = ::compare(k.ikey_u.ikey[1], keys1_[index]);
    if (cmp == 0) {
      cmp = ::compare(k.ikey_u.ikey[0], keys0_[index]);
    }
    return cmp;
  }

  void assign(const key_type& k, value_type v, size_t index, threadinfo& ti) {
    value_type* pNewValue = (value_type*) ti.pool_allocate(sizeof(value_type), memtag_value);
    new (pNewValue) value_type(v);
    lk_.lock();
    keys1_[index] = k.ikey_u.ikey[1];
    keys0_[index] = k.ikey_u.ikey[0];
    values_[index] = pNewValue;
    lk_.unlock();
  }
};

template <typename P> void four_tree<P>::initialize(threadinfo &ti) {
  root_ = node_type::make(ti);
}

template <typename P> class cursor {
public:
  using node_type = node<P>;
  using value_type = typename P::value_type;
  using key_type = fix_sized_key<P::ikey_size>;
  using threadinfo = typename P::threadinfo_type;

  cursor(const four_tree<P> &tree, Str key)
      : parent_(nullptr), node_(nullptr), k_(key), pv_(nullptr), root_(tree.root()) {}

  cursor(const four_tree<P> &tree, const char *key)
      : parent_(nullptr), node_(nullptr), k_(key_type(key)), pv_(nullptr), root_(tree.root()) {}

  bool find() {
    node_ = const_cast<node_type *>(root_);

    forward:
    while (node_) {
      node_->lk_.lock();
      for (int i=0; i<P::fanout-1; i++) {
        // value not exists
        if (node_->values_[i] == nullptr) {
          index_ = i;
          pv_ = node_->values_[index_];
          node_->lk_.unlock();
          return false;
        }
        int cmp = node_->compare_with(k_, i);
        if (cmp == 0) {
          index_ = i;
          pv_ = node_->values_[i];
          node_->lk_.unlock();
          return true;
        } else if (cmp < 0) {
          parent_ = node_;
          node_ = node_->children_[i];
          index_ = i;
          parent_->lk_.unlock();
          goto forward;
        }
      }
      parent_ = node_;
      node_ = node_->children_[P::fanout-1];
      parent_->lk_.unlock();
    }
    // node_ = null, i.e. new node
    // keep the last index
    return false;
  }

private:
  node_type *parent_;
  node_type *node_;
  size_t index_;
  key_type k_;
  value_type *pv_;
  const node_type *root_;

  friend class query;
};

class query {
public:
  template <typename T> typename T::value_type *get(T &tree, Str key) {
    typename T::cursor_type c(tree, key);
    if (c.find()) {
      return c.pv_;
    } else {
      return nullptr;
    }
  }

  template <typename T>
  void put(T &tree, Str key, typename T::value_type value, threadinfo &ti) {
    using value_type = typename T::value_type;
    using node_type = typename T::node_type;

    typename T::cursor_type c(tree, key);
    bool found = c.find();
    if (c.node_ == nullptr) {
      // create new node and save to the first
      node_type* node = node_type::make(ti);
      node->assign(key, value, 0, ti);
      c.parent_->lk_.lock();
      c.parent_->children_[c.index_] = node;
      c.parent_->lk_.unlock();
    } else {
      if (found) {
        // update the value
        c.node_->lk_.lock();
        *c.node_->values_[c.index_] = value;
        c.node_->lk_.unlock();
      } else {
        c.node_->assign(key, value, c.index_, ti);
      }
    }

  }
};

using default_table = four_tree<tree_params<16>>;
// static_assert(sizeof(default_table::node_type) == 40,
//               "binary tree node size is not 40 bytes");
} // namespace binarytree
#endif