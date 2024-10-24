#pragma once

#include <cerrno>
#include <cmath>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <stack>
#include <string>
#include <string_view>
#include <tuple>

#include <fmt/format.h>
#include <glog/logging.h>

#include "trie_sort_base.h"

namespace whippet_sort::trie_v2 {

using ValueT = int;

constexpr size_t kElementNum = 256;

struct TrieConfig {
  // lazy node brust limit, 0 means no lazy node
  int lazy_key_burst_lmt = 4096;
};

struct Trie {
  struct Node {
    const size_t node_id;
    Node *const parent{nullptr};
    const size_t plen{0}; // the length of the prefix
    std::string str;

    // first: prefix length, second: char, third: child node
    std::vector<std::tuple<int, uint8_t, Node *>> children_l, children_g;

    // first: prefix length, second: value (including the str of this node)
    std::vector<std::pair<int, ValueT>> substr_values;

    // lazy values, <prefix_len, string, value>
    std::deque<std::tuple<size_t, std::string, ValueT>> lazy_values;
    bool is_lazy = false;
  };

  auto createNode(Node *parent, size_t plen, std::string str, ValueT value) {
    auto new_node = std::make_unique<Node>(
        Node{.node_id = node_num++, .parent = parent, .plen = plen});
    new_node->substr_values.emplace_back(str.length(), value);
    new_node->str = std::move(str);

    return this->node_pool_.emplace_back(std::move(new_node)).get();
  }

  std::array<Node *, kElementNum> roots{nullptr};
  size_t value_num = 0;
  size_t node_num = 0;

  std::deque<std::unique_ptr<Node>> node_pool_;
};

namespace {
template <typename T>
size_t prefix_eq_len(const T &x, size_t x_pos, const std::string &y,
                     size_t y_pos) {
  size_t i = 0;
  auto len = std::min(x.length() - x_pos, y.length() - y_pos);
  using CmpT = size_t;
  constexpr auto gap = sizeof(CmpT) / sizeof(uint8_t);
  for (; i + gap < len; i += gap) {
    auto p1 = *reinterpret_cast<const CmpT *>(x.data() + i + x_pos);
    auto p2 = *reinterpret_cast<const CmpT *>(y.data() + i + y_pos);
    if (auto t = p1 ^ p2) {
      break;
    }
  }
  for (; i < len; ++i) {
    if (x[i + x_pos] != y[i + y_pos]) {
      return i;
    }
  }

  return i;
}
} // namespace

class TriePrinter;

class TrieBuilder : public TrieBuilderBase {
protected:
  using Node = Trie::Node;
  friend class TriePrinter;

public:
  TrieBuilder(TrieConfig config = {})
      : trie_(std::make_unique<Trie>()), config_(config) {
    reset();
  };

  using TrieBuilderBase::insert;

  /**
   * @brief Insert a key into the tree
   * @param prefix_len The length of the prefix shared with last inserted key
   * @param key The suffix of the key to insert
   * @param value The value to the key
   */
  void insert(size_t prefix_len, const std::string_view &key,
              ValueT value) override {
    if (prefix_len == 0) {
      if (trie_->roots[key[0]] == nullptr) {
        auto new_node = trie_->createNode(nullptr, 0, std::string(key), value);
        curr_node_ = trie_->roots[key[0]] = new_node;

        curr_length_ = key.length();
        curr_depth_ = 1;
        return;
      }
      curr_node_ = trie_->roots[key[0]];

      curr_length_ = curr_node_->str.length();
      curr_depth_ = 1;

    } else {
      // Jump up to the node that owns the deepest prefix with the inserted key
      while (curr_node_->plen >= prefix_len) {
        curr_length_ = curr_node_->plen;
        curr_node_ = curr_node_->parent;
        --curr_depth_;
      }
    }

    size_t key_i = 0;
    while (1) {
      if (curr_node_->is_lazy) {
        curr_node_->lazy_values.emplace_back(
            prefix_len + key_i, std::string(key.substr(key_i)), value);
        curr_length_ = curr_node_->plen + curr_node_->str.length();
        if (curr_node_->lazy_values.size() > config_.lazy_key_burst_lmt) {
          burstLazyNode(curr_node_);
        }
        return;
      }

      DCHECK_GE(curr_length_, prefix_len)
          << "prefix_len too large! prefix_len: " << prefix_len
          << ", curr_length_: " << curr_length_;
      // Check if we need to split curr_node_->str

      auto curr_skip_len = prefix_len + key_i > curr_node_->plen
                               ? prefix_len + key_i - curr_node_->plen
                               : 0;
      size_t same_len =
          prefix_eq_len(key, key_i, curr_node_->str, curr_skip_len);
      key_i += same_len;
      curr_skip_len += same_len;
      DCHECK_LE(curr_skip_len, curr_node_->str.length());
      DCHECK_LT(0, curr_skip_len);

      if (key_i == key.length()) {
        curr_node_->substr_values.emplace_back(curr_skip_len, value);
        curr_length_ = curr_node_->plen + curr_node_->str.length();
        return;
      } else if (curr_skip_len == curr_node_->str.length()) {
        curr_node_->str.append(key.substr(key_i));
        curr_node_->substr_values.emplace_back(curr_node_->str.length(), value);
        curr_length_ = curr_node_->plen + curr_node_->str.length();
        return;
      } else {
        auto children = &curr_node_->children_l;
        auto ch = key[key_i];
        auto pos = children->end();

        // TODO: std::lower_bound can be optimized
        if (curr_node_->str[curr_skip_len] < key[key_i]) {
          // key is greater than curr_node_->str
          children = &curr_node_->children_g;
          pos = std::lower_bound(children->begin(), children->end(),
                                 std::make_tuple(curr_skip_len, ch, nullptr),
                                 [](auto &x, auto &y) {
                                   return std::get<0>(x) > std::get<0>(y) ||
                                          (std::get<0>(x) == std::get<0>(y) &&
                                           std::get<1>(x) < std::get<1>(y));
                                 });
        } else {
          children = &curr_node_->children_l;
          pos = std::lower_bound(children->begin(), children->end(),
                                 std::make_tuple(curr_skip_len, ch, nullptr),
                                 [](auto &x, auto &y) {
                                   return std::get<0>(x) > std::get<0>(y) ||
                                          (std::get<0>(x) == std::get<0>(y) &&
                                           std::get<1>(x) > std::get<1>(y));
                                 });
        }
        auto &[len, c, child_node] = *pos;
        if (pos != children->end() && len == curr_skip_len && c == ch) {
          // go to the child node and continue
          curr_length_ = curr_node_->plen + curr_skip_len;
          curr_node_ = child_node;
          curr_depth_++;
          DCHECK_EQ(curr_length_, curr_node_->plen);
        } else {
          // create a new node
          auto new_node = trie_->createNode(
              curr_node_, curr_node_->plen + curr_skip_len,
              std::string(key_i ? key.substr(key_i) : key), value);
          { new_node->is_lazy = true; }

          children->insert(pos, std::make_tuple(curr_skip_len, ch, new_node));

          curr_length_ = new_node->plen + new_node->str.length();
          curr_node_ = new_node;
          curr_depth_++;
          return;
        }
      }
    }
    DCHECK(false) << "should not reach here";
    // TODO: break down the edge if too long (too many elements).
  }

  size_t valueNum() const override { return trie_->value_num; }

  std::unique_ptr<Trie> build() {
    auto ret = std::make_unique<Trie>();
    ret.swap(trie_);
    reset();
    return ret;
  }

protected:
  void burstLazyNode(Node *node) {
    DCHECK(node->is_lazy);
    auto &lazy_values = node->lazy_values;
    node->is_lazy = false;
    for (auto &[prefix_len, key, value] : lazy_values) {
      insert(prefix_len, key, value);
    }
    lazy_values.clear();
  }

  void reset() {
    trie_ = std::make_unique<Trie>();
    curr_node_ = nullptr;
    curr_length_ = 0;
    curr_depth_ = 0;
  };

  std::unique_ptr<Trie> trie_;
  TrieConfig config_;

private:
  Node *curr_node_;
  size_t curr_length_; // length of string
  size_t curr_depth_;  // number of nodes
};

class TrieBuilderBfs : public TrieBuilder {
public:
  TrieBuilderBfs(TrieConfig config = {}) : TrieBuilder(std::move(config)) {}

  void insert(size_t prefix_len, const std::string_view &key,
              ValueT value) override {
    throw std::runtime_error("not implemented");
  }

  void
  insert(std::vector<std::tuple<size_t, std::string_view, int>> keys) override {
    CHECK(!keys.empty());
    CHECK(std::get<0>(keys.front()) == 0);
    char ch;
    Node *curr_node = nullptr;
    for (auto &[prefix_len, key, value] : keys) {
      if (prefix_len == 0) {
        DCHECK(!key.empty());
        ch = key[0];
        if (trie_->roots[ch] == nullptr) {
          auto new_node =
              trie_->createNode(nullptr, 0, std::string(key), value);
          lazy_keys_.emplace_back();
          curr_node = trie_->roots[ch] = new_node;
          continue;
        }
        curr_node = trie_->roots[ch];
      }
      lazy_keys_[curr_node->node_id].emplace_back(prefix_len, std::move(key),
                                                  value);
      to_visit_nodes_.push_back(curr_node);
    }
    while (!to_visit_nodes_.empty()) {
      auto node = to_visit_nodes_.front();
      to_visit_nodes_.pop_front();

      auto nid = node->node_id;
      if (!lazy_keys_[nid].empty()) {
        insert_impl(node);
      }
    }
    to_visit_nodes_.clear();
  }

protected:
  void insert_impl(Node *curr_node) {
    DCHECK_LT(curr_node->node_id, lazy_keys_.size())
        << "nid: " << curr_node->node_id
        << ", node_keys_.size(): " << lazy_keys_.size();
    auto curr_length = curr_node->plen + curr_node->str.length();
    size_t str_skip_len = 0;
    decltype(lazy_keys_)::value_type *last_key_go = nullptr;
    for (auto &[prefix_len, key, value] : lazy_keys_[curr_node->node_id]) {
      DCHECK_LE(curr_node->plen, prefix_len)
          << "prefix_len too small! prefix_len: " << prefix_len
          << ", curr_node->plen: " << curr_node->plen;
      if (last_key_go && curr_node->plen + str_skip_len < prefix_len) {
        // go where last key go
        last_key_go->emplace_back(prefix_len, std::move(key), value);
        continue;
      }
      DCHECK_GE(curr_length, prefix_len)
          << "prefix_len too large! prefix_len: " << prefix_len
          << ", curr_length: " << curr_length;

      last_key_go = nullptr;
      str_skip_len =
          prefix_len > curr_node->plen ? prefix_len - curr_node->plen : 0;
      auto same_len = prefix_eq_len(key, 0, curr_node->str, str_skip_len);

      str_skip_len += same_len;
      DCHECK_LE(str_skip_len, curr_node->str.length());
      DCHECK_LT(0, str_skip_len);

      if (same_len == key.length()) {
        curr_node->substr_values.emplace_back(str_skip_len, value);
        continue;
      } else if (str_skip_len == curr_node->str.length()) {
        curr_node->str.append(key.substr(same_len));
        curr_node->substr_values.emplace_back(curr_node->str.length(), value);
        curr_length = curr_node->plen + curr_node->str.length();
        continue;
      } else {
        auto children = &curr_node->children_l;
        auto ch = key[same_len];
        auto pos = children->end();

        if (curr_node->str[str_skip_len] < key[same_len]) {
          //  key is greater than curr_node->str
          children = &curr_node->children_g;
          pos = std::lower_bound(children->begin(), children->end(),
                                 std::make_tuple(str_skip_len, ch, nullptr),
                                 [](auto &x, auto &y) {
                                   return std::get<0>(x) > std::get<0>(y) ||
                                          (std::get<0>(x) == std::get<0>(y) &&
                                           std::get<1>(x) < std::get<1>(y));
                                 });
        } else {
          children = &curr_node->children_l;
          pos = std::lower_bound(children->begin(), children->end(),
                                 std::make_tuple(str_skip_len, ch, nullptr),
                                 [](auto &x, auto &y) {
                                   return std::get<0>(x) > std::get<0>(y) ||
                                          (std::get<0>(x) == std::get<0>(y) &&
                                           std::get<1>(x) > std::get<1>(y));
                                 });
        }
        auto &[len, c, child_node] = *pos;
        if (pos != children->end() && len == str_skip_len && c == ch) {
          // go to the child node and continue
          auto new_nid = child_node->node_id;
          prefix_len += same_len;
          lazy_keys_[new_nid].emplace_back(prefix_len, key.substr(same_len),
                                           value);
          last_key_go = &lazy_keys_[new_nid];
          to_visit_nodes_.push_back(child_node);
        } else {
          // create a new node
          if (same_len) {
            key = key.substr(same_len);
          }
          auto new_node =
              trie_->createNode(curr_node, curr_node->plen + str_skip_len,
                                std::string(key), value);
          children->insert(pos, std::make_tuple(str_skip_len, ch, new_node));

          lazy_keys_.emplace_back();
          last_key_go = &lazy_keys_[new_node->node_id];
          to_visit_nodes_.push_back(new_node);
          continue;
        }
      }
    }
    lazy_keys_[curr_node->node_id].clear();
  }

  std::deque<Node *> to_visit_nodes_;
  std::vector<std::vector<std::tuple<size_t, std::string_view, int>>>
      lazy_keys_;
};

class TriePrinter {
  using ValueT = int;

  friend class TrieBuilder;

  using Node = Trie::Node;

public:
  // func(prefix_len, key, value)
  using FuncT = std::function<void(size_t, std::string, ValueT)>;

  TriePrinter(std::unique_ptr<Trie> &&trie) : trie_(std::move(trie)) {}

  void registerFunc(FuncT func) { func_ = std::move(func); }

  void preSort() {
    CHECK(!pre_sorted);
    node_lazy_keys_.resize(trie_->node_num);
    for (auto &node : trie_->node_pool_) {
      // auto nid = node->node_id;
      std::sort(node->substr_values.begin(), node->substr_values.end(),
                [](auto &x, auto &y) { return x.first > y.first; });

      if (node->is_lazy) {
        handleLazyNode(node.get(), node_lazy_keys_[node->node_id]);
      }
#ifndef NDEBUG
      for (int i = 1; i < node->children_l.size(); ++i) {
        DCHECK(std::get<0>(node->children_l[i]) <
                   std::get<0>(node->children_l[i - 1]) ||
               (std::get<0>(node->children_l[i]) ==
                    std::get<0>(node->children_l[i - 1]) &&
                std::get<1>(node->children_l[i]) <
                    std::get<1>(node->children_l[i - 1])));
      }
      for (int i = 1; i < node->children_g.size(); ++i) {
        DCHECK(std::get<0>(node->children_g[i]) <
                   std::get<0>(node->children_g[i - 1]) ||
               (std::get<0>(node->children_g[i]) ==
                    std::get<0>(node->children_g[i - 1]) &&
                std::get<1>(node->children_g[i]) >
                    std::get<1>(node->children_g[i - 1])));
      }
#endif
    }
    pre_sorted = true;
  }

  void print() {
    CHECK(pre_sorted);
    for (auto &root : trie_->roots) {
      if (!root)
        continue;
      prefix_.clear();
      last_prefix_len_ = 0;
      prefix_stack_.emplace(root, 0, 0);

      while (!prefix_stack_.empty()) {
        auto &[node, node_prefix_len, idx] = prefix_stack_.top();
        // node_prefix_len is the length of the prefix inside the node->str
        // idx is the index of the child node in the children_g

        if (node->is_lazy) {
          // handleLazyNode(node);
          for (auto &[key, value] : node_lazy_keys_[node->node_id]) {
            prefix_.append(std::move(key));
            print_string(value);
            last_prefix_len_ -= key.length();
          }

          prefix_stack_.pop();
          continue;
        }

        // print the value that less than current node
        if (!node->children_l.empty()) {
          auto [len, ch, child_node] = node->children_l.back();
          DCHECK(!node->substr_values.empty());
          auto [v_len, value] = node->substr_values.back();
          if (v_len <= len) {
            node->substr_values.pop_back();
            prefix_.append(node->str.data() + node_prefix_len,
                           v_len - node_prefix_len);
            node_prefix_len = v_len;
            // func_(0, prefix_, value);
            print_string(value);
          } else {
            node->children_l.pop_back();
            prefix_.append(node->str.data() + node_prefix_len,
                           len - node_prefix_len);
            node_prefix_len = len;
            prefix_stack_.emplace(std::make_tuple(child_node, 0, 0));
          }
          continue;
        }

        // print the value of current node
        while (!node->substr_values.empty()) {
          auto [v_len, value] = node->substr_values.back();
          node->substr_values.pop_back();
          prefix_.append(node->str.data() + node_prefix_len,
                         v_len - node_prefix_len);
          node_prefix_len = v_len;
          // func_(0, prefix_, value);
          print_string(value);
        }

        // print the value that greater than current node
        if (idx < node->children_g.size()) {
          auto [len, ch, child_node] = node->children_g[idx++];
          // prefix_.resize(prefix_.size() - node_prefix_len + len);
          last_prefix_len_ -= node_prefix_len - len;
          node_prefix_len = len;
          prefix_stack_.emplace(std::make_tuple(child_node, 0, 0));
          continue;
        }
        // prefix_.resize(prefix_.size() - node_prefix_len);
        last_prefix_len_ -= node_prefix_len;
        prefix_stack_.pop();
      }
    }
  }

  auto valueNum() const { return trie_->value_num; }

private:
  void handleLazyNode(Node *node,
                      std::vector<std::pair<std::string, ValueT>> &lazy_keys) {
    auto last_str_len = node->plen + node->str.length();
    // notice: pre_len might be 0
    std::string last_key = node->str;
    lazy_keys.reserve(node->lazy_values.size() + 1);
    lazy_keys.emplace_back(last_key, node->substr_values.back().second);
    CHECK_EQ(node->substr_values.size(), 1);

    for (auto &[prefix_len, key, value] : node->lazy_values) {
      DCHECK_LE(prefix_len, last_str_len);
      int64_t key_i = prefix_len - node->plen;
      last_key = last_key.substr(0, key_i) + key;
      lazy_keys.emplace_back(last_key, value);
      last_str_len = prefix_len + key.length();
    }
    std::sort(lazy_keys.begin(), lazy_keys.end(),
              [](auto &x, auto &y) { return x.first < y.first; });
  }

  void print_string(int value) {
    func_(last_prefix_len_, prefix_, value);
    last_prefix_len_ += prefix_.length();
    prefix_.clear();
  }

  std::unique_ptr<Trie> trie_;
  FuncT func_;

  bool pre_sorted = false;
  std::string prefix_;
  size_t last_prefix_len_ = 0;

  std::stack<std::tuple<Node *, size_t, size_t>> prefix_stack_;

  std::vector<std::pair<std::string, ValueT>> lazy_keys_;
  std::vector<std::vector<std::pair<std::string, ValueT>>> node_lazy_keys_;
};

} // namespace whippet_sort::trie_v2