#ifndef AVLTREE_HH
#define AVLTREE_HH

#include "circular_int.hh"
#include "compiler.hh"
#include "fixsizedkey.hh"
#include "kvthread.hh"
#include "mtcounters.hh"
#include "str.hh"
#include "string.hh"
#include <stdio.h>
#include <string>
#include <stack>

namespace avltree {
using lcdf::Str;
using lcdf::String;
typedef uint32_t height_t;

template<size_t KeySize = 16>
struct tree_params {
    static constexpr int ikey_size = KeySize;
    static constexpr bool enable_int_cmp = true;
    using value_type = std::string;
    using threadinfo_type = ::threadinfo;
};


template<typename P>
class node;

template<typename P>
class cursor;

template<typename P>
class avl_tree {
public:
    using parameter_type = P;
    using node_type = node<P>;
    using value_type = typename P::value_type;
    using key_type = fix_sized_key<P::ikey_size, P::enable_int_cmp>;
    using threadinfo = typename P::threadinfo_type;
    using cursor_type = cursor<P>;

    inline avl_tree() {}

    void initialize(threadinfo &ti);

    void destroy(threadinfo &ti);

    inline node_type *root() const { return root_; }

    static const char *name() { return "avltree"; }

    void print(FILE *f) const {
        print(f, root_);
    }

private:
    node_type *root_;

    void print(FILE *f, node_type *node) const {
        print(f, "", node, false);
    }

    void print(FILE *f, const std::string &prefix, const node_type *node, bool isLeft) const {
        if (node != nullptr) {
            fprintf(f, "%s", prefix.c_str());

            fprintf(f, "%s", (isLeft ? "├──" : "└──"));

            // print the value of the node
            fprintf(f, "%s:%s\n", node->key_.unparse_printable().c_str(),
                    (node->pValue_ ? node->pValue_->data() : "null"));

            // enter the next tree level - left and right branch
            print(f, prefix + (isLeft ? "│   " : "    "), node->pLeft_, true);
            print(f, prefix + (isLeft ? "│   " : "    "), node->pRight_, false);
        }
    }
};

template<typename P>
class alignas(CACHE_LINE_SIZE)

node {
using key_type = fix_sized_key<P::ikey_size, P::enable_int_cmp>;
using value_type = typename P::value_type;
using threadinfo = typename P::threadinfo_type;
using node_type = node<P>;

public:
key_type key_;
value_type *pValue_;
node<P> *pLeft_;
node<P> *pRight_;
height_t height_ = 0;

node_type *child(int direction) {
    assert(direction != 0);

    if (direction > 0)
        return pRight_;

    return pLeft_;
}

static inline height_t get_height(node_type *node) {
    return node == nullptr ? 0 : node->height_;
}

void recompute_height() {
    height_ = max(get_height(pLeft_), get_height(pRight_)) + 1;
}

node(Str
     k)
        :

        key_(key_type(k)), pValue_(nullptr), pLeft_(nullptr), pRight_(nullptr) {
}

static node_type *make(Str
                       key,
                       threadinfo &ti) {
    node_type *data = (node_type *) ti.pool_allocate(sizeof(node_type), memtag_masstree_internode);
    new(data)
            node_type(key);
    return
            data;
}
};

template<typename P>
void avl_tree<P>::initialize(threadinfo &ti) {
    root_ = node_type::make("", ti);
}

template<typename P>
class cursor {
public:
    using node_type = node<P>;
    using value_type = typename P::value_type;
    using key_type = fix_sized_key<P::ikey_size, P::enable_int_cmp>;
    using threadinfo = typename P::threadinfo_type;

    cursor(const avl_tree <P> &tree, Str key)
            : parent_(nullptr), node_(nullptr), k_(key), pv_(nullptr), root_(tree.root()) {}

    cursor(const avl_tree <P> &tree, const char *key)
            : parent_(nullptr), node_(nullptr), k_(key_type(key)), pv_(nullptr), root_(tree.root()) {}

    bool find() {
        node_ = const_cast<node_type *>(root_);
        do {
            acquire_fence();
            int cmp = k_.compare(node_->key_);
            if (cmp == 0) {
                pv_ = node_->pValue_;
                return true;
            } else {
                parent_ = node_;
                node_ = node_->child(cmp);
                assert(parent_);
            }
        } while (node_);
        return false;
    }

    pair<stack<node_type>, bool> find_with_stack() {
        node_ = const_cast<node_type *>(root_);
        stack <node_type> stk;

        do {
            acquire_fence();
            stk.push(node_);
            int cmp = k_.compare(node_->key_);
            if (cmp == 0) {
                pv_ = node_->pValue_;
                return make_pair(stk, true);
            } else {
                parent_ = node_;
                node_ = node_->child(cmp);
                assert(parent_);
            }
        } while (node_);

        return make_pair(stk, false);
    }

private:
    node_type *parent_;
    node_type *node_;
    key_type k_;
    value_type *pv_;
    const node_type *root_;

    friend class query;
};

class query {
public:
    template<typename T>
    typename T::value_type *get(T &tree, Str key) {
        typename T::cursor_type c(tree, key);
        if (c.find()) {
            return c.pv_;
        } else {
            return nullptr;
        }
    }

    template<typename T>
    void put(T &tree, Str key, typename T::value_type value, threadinfo &ti) {
        using value_type = typename T::value_type;
        using node_type = typename T::node_type;

        // allocate new value ptr
        value_type *newValue = (value_type *) ti.pool_allocate(sizeof(value_type), memtag_value);
        new(newValue) value_type(value);

        retry:
        typename T::cursor_type c(tree, key);
        node_type *newNode = nullptr;
        auto ret = c.find();
        stack <node_type> stk = ret.first;
        bool found = ret.second;

        if (found) {
            // perform update
            release_fence();
            while (true) {
                if (bool_cmpxchg<value_type *>(&c.node_->pValue_, c.pv_, newValue)) {
                    break;
                }
                c.pv_ = c.node_->pValue_;
                relax_fence();
            }
            ti.pool_deallocate(c.pv_, sizeof(value_type), memtag_value);
        } else {
            // perform insert
            if (!newNode) {
                newNode = node_type::make(key, ti);
                newNode->pValue_ = newValue;
            }
            release_fence();
            int dir = c.k_.compare(c.parent_->key_);
            if (bool_cmpxchg<node_type *>(dir > 0 ? &c.parent_->pRight_
                                                  : &c.parent_->pLeft_,
                                          nullptr, newNode)) {
                c.node_ = newNode;

                // Balance, update height
                while (!stk.empty()) {
                    auto cur = stk.pop();
                    balance_height(cur);
                    cur->recompute_height();
                }
            } else {
                goto retry;
            }
        }
    }

    node_type *left_rotate(node_type *nd) {
        auto right_node = nd->pRight_;

        nd->pRight_ = right_node->pLeft_;
        right_node->pLeft_ = nd;

        nd->recompute_height();
        right_node->recompute_height();

        return right_node;
    }

    node_type *right_rotate(node_type *nd) {
        auto left_node = nd->pLeft_;

        nd->pLeft_ = left_node->pRight_;
        left_node->pRight_ = nd;

        nd->recompute_height();
        left_node->recompute_height();

        return left_node;
    }

    static height_t get_balance_factor(node_type *nd) {
        return node::get_height(nd->pLeft_) - node::get_height(nd->pRight_);
    }

    void balance_height(node_type *nd) {
        if (nd == nullptr) {
            return nullptr;
        }

        auto balance_factor = get_balance_factor(nd);

        if (abs(balance_factor) <= 1) return nd; // No need to balance

        if (balance_factor > 0) {
            // Left height greater
            auto left_balance_factor = get_balance_factor(nd->pLeft_);
            if (left_balance_factor > 0) {
                // left-left case
                return right_rotate(nd);
            } else {
                // left-right case
                nd->pLeft_ = left_rotate(nd->pLeft_);
                return right_rotate(nd);
            }
        } else {
            // Right height greater
            auto right_balance_factor = get_balance_factor(nd->pLeft_);
            if (right_balance_factor < 0) {
                // right-right case
                return left_rotate(nd);
            } else {
                // right-left case
                nd->pRight_ = right_rotate(nd->pRight_);
                return left_rotate(nd);
            }
        }
    }
};

using default_table = avl_tree <tree_params<16>>;
// static_assert(sizeof(default_table::node_type) == 40,
//               "binary tree node size is not 40 bytes");
} // namespace avltree
#endif