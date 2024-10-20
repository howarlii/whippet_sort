#pragma once

#include <cerrno>
#include <cmath>
#include <cstddef>
#include <deque>
#include <memory>
#include <stack>
#include <string>
#include <string_view>

#include "semistring.h"
#include "trie_sort_base.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/types.h>

namespace whippet_sort::trie {

using namespace trie__internal;

struct TrieConfig {
  // lazy node are allowed when depth is bigger than lazy_dep_lmt
  int lazy_dep_lmt = 4;

  // lazy node brust limit
  int lazy_key_burst_lmt = 4096;
};

template <typename ValueT> struct Trie {

  struct LazyKey {
    size_t prefix_len; // The length of the prefix shared with last LazyKey
    SemiStringView key;
    ValueT value;
  };

  struct Node {
    bool is_lazy_node{false};
    std::deque<LazyKey> lazy_keys;
    std::unique_ptr<Node> children[kElementNum]{nullptr};
    Node *parent{nullptr};
    SemiStringView str; // string on the edge to the parent, if this is a lazy
                        // node, str is the shared prefix
    size_t pdep{0};     // the length of the prefix of parent
    std::vector<ValueT> values;
  };

  std::unique_ptr<Node> root_{nullptr};
  size_t value_num_;

  std::deque<std::string> str_pool_; // TODO: to be optimized
  // std::deque<std::string> str_pool_; // slower, why?
};

class TriePrinter;

class TrieBuilder : public TrieBuilderBase {
  using ValueT = int;

  using LazyKey = Trie<ValueT>::LazyKey;
  using Node = Trie<ValueT>::Node;
  friend class TriePrinter;

  std::unique_ptr<Trie<ValueT>::Node> createNewNode(SemiStringView key,
                                                    ValueT &&value);
  void addLazyKey(Node *node, LazyKey &&lazy_key) {
    size_t shared_pref_len = node->str.length();
    auto &key = lazy_key.key;
    if (lazy_key.prefix_len) {
      if (lazy_key.prefix_len < node->str.length()) {
        shared_pref_len =
            lazy_key.prefix_len +
            key.prefix_eq_len(node->str.substr_tail(lazy_key.prefix_len));
      }
    } else {
      shared_pref_len = key.prefix_eq_len(node->str);
    }
    if (shared_pref_len < node->str.length()) {
      node->str = node->str.substr(0, shared_pref_len);
    }
    node->lazy_keys.emplace_back(std::move(lazy_key));
  }

public:
  TrieBuilder(TrieConfig config = {})
      : trie_(std::make_unique<Trie<ValueT>>()), config_(config) {
    reset();
  };

  /**
   * @brief Insert a key into the tree
   * @param prefix_len The length of the prefix shared with last inserted key
   * @param key The suffix of the key to insert
   * @param value The value to the key
   */
  void insert(size_t prefix_len, const std::string_view &key_in,
              ValueT value) override {
    ++trie_->value_num_;
    const auto &str = trie_->str_pool_.emplace_back(key_in);
    SemiStringView key(str);
    prefix_len *= kTranF;

    // LOG(INFO) << "insert: " << key_in << " " << value;
    insert_impl(prefix_len, std::move(key), value);
  }

  void insert_impl(size_t prefix_len, SemiStringView &&key, ValueT value) {
    if (prefix_len == 0) {
      curr_node_ = trie_->root_.get();
      curr_length_ = 0;
      curr_depth_ = 0;
    } else {
      // Jump up to the node that owns the deepest prefix with the inserted key
      while (curr_node_->pdep >= prefix_len) {
        curr_length_ = curr_node_->pdep;
        curr_node_ = curr_node_->parent;
        --curr_depth_;
      }
    }

    size_t key_i = 0;
    while (1) {
      if (curr_node_->is_lazy_node) {
        auto curr_skip_pref_len =
            (prefix_len > curr_node_->pdep) ? prefix_len - curr_node_->pdep : 0;
        addLazyKey(curr_node_,
                   Trie<ValueT>::LazyKey{.prefix_len = curr_skip_pref_len,
                                         .key = key.substr_tail(key_i),
                                         .value = value});
        curr_length_ = curr_node_->pdep + curr_node_->str.length();
        if (curr_node_->lazy_keys.size() > config_.lazy_key_burst_lmt) {
          burstLazyNode(curr_node_);
        }
        return;
      }

      DCHECK_GE(curr_length_, prefix_len)
          << "prefix_len too large! prefix_len: " << prefix_len
          << ", curr_length_: " << curr_length_;
      // Check if we need to split curr_node_->str
      if (curr_length_ > prefix_len) {
        auto curr_skip_pref_len =
            (prefix_len > curr_node_->pdep) ? prefix_len - curr_node_->pdep : 0;
        size_t same_len;
        if (curr_skip_pref_len) {
          same_len = key.substr_tail(key_i).prefix_eq_len(
              curr_node_->str.substr_tail(curr_skip_pref_len));
        } else {
          same_len = key.substr_tail(key_i).prefix_eq_len(curr_node_->str);
        }

        key_i += same_len;
        if (curr_skip_pref_len + same_len < curr_node_->str.length()) {
          // Split curr_node_->str into two parts and create a new node for the
          // second part
          auto new_node_u = std::make_unique<Node>();
          auto new_node = new_node_u.get();
          new_node->str =
              curr_node_->str.substr(0, curr_skip_pref_len + same_len);
          new_node->parent = curr_node_->parent;
          new_node->pdep = curr_node_->pdep;

          curr_node_->str =
              curr_node_->str.substr_tail(curr_skip_pref_len + same_len);
          curr_node_->parent = new_node;
          curr_node_->pdep += curr_skip_pref_len + same_len;

          std::swap(new_node->parent->children[new_node->str[0]], new_node_u);
          DCHECK_EQ(new_node_u.get(), curr_node_);
          new_node->children[new_node_u->str[0]] = std::move(new_node_u);

          curr_length_ = curr_node_->pdep;
          curr_node_ = new_node;
        }
      }

      // Here we on the node curr_node_ that owns exaclly the same prefix with
      // the key
      if (key_i == key.length()) {
        // The key is already in the tree, insert the value
        curr_node_->values.push_back(value);
        return;
      } else if (curr_node_->children[key[key_i]] == nullptr) {
        // Create a new node for the remaining suffix of the key
        auto new_node = (curr_node_->children[key[key_i]] = createNewNode(
                             key.substr_tail(key_i), std::move(value)))
                            .get();
        curr_depth_++;
        curr_length_ += new_node->str.length();
        curr_node_ = new_node;

        return;
      } else {
        // Move to the child node
        curr_node_ = curr_node_->children[key[key_i]].get();
        curr_length_ += curr_node_->str.length();
        curr_depth_++;
      }
    }
    DCHECK(false) << "should not reach here";
  }

  size_t valueNum() const override { return trie_->value_num_; }

  std::unique_ptr<Trie<ValueT>> build() {
    auto ret = std::make_unique<Trie<ValueT>>();
    ret.swap(trie_);
    reset();
    return ret;
  }

private:
  void reset() {
    trie_->root_ = std::make_unique<Node>();
    trie_->root_->pdep = 0;
    trie_->value_num_ = 0;
    curr_node_ = trie_->root_.get();
    curr_length_ = 0;
    curr_depth_ = 0;
  };

  void burstLazyNode(Node *node) {
    node->is_lazy_node = false;
    auto pref_len = node->str.length();
    auto pdep = node->pdep;
    DCHECK_GE(pref_len, 1);
    for (auto &lazy_key : node->lazy_keys) {
      if (pref_len > lazy_key.prefix_len) {
        insert_impl(pdep + pref_len,
                    lazy_key.key.substr_tail(pref_len - lazy_key.prefix_len),
                    std::move(lazy_key.value));
      } else {
        insert_impl(pdep + lazy_key.prefix_len, std::move(lazy_key.key),
                    std::move(lazy_key.value));
      }
    }
    node->lazy_keys.clear();
  }

  std::unique_ptr<Trie<ValueT>> trie_;

  Node *curr_node_;
  size_t curr_length_;
  size_t curr_depth_;

  TrieConfig config_;
};

class TriePrinter {
  using ValueT = int;

  friend class TrieBuilder;

  using Node = Trie<ValueT>::Node;

public:
  TriePrinter(std::unique_ptr<Trie<ValueT>> &&trie) : trie_(std::move(trie)) {
    prefix_stack_.emplace(trie_->root_.get(), 0);
  }

  bool hasNext() const { return !prefix_stack_.empty(); }

  bool next(size_t *prefix_len, std::string *key, int *values) {
    if (!lazy_keys_.empty()) {
      *prefix_len = prefix_str_len_ / kTranF;
      *key = std::move(lazy_keys_.back().first);
      *values = std::move(lazy_keys_.back().second);
      lazy_keys_.pop_back();
      return true;
    }

    if (!hasNext())
      return false;

    auto &[node_r, idx_r] = prefix_stack_.top();
    Node *node = node_r;
    uint8_t *idx = &idx_r;

    SemiString suf_str;
    while (node->values.empty()) {
      for (; *idx < kElementNum && node->children[*idx] == nullptr; ++(*idx))
        ;
      if (*idx < kElementNum) {
        node = node->children[*idx].get();
        ++(*idx);
        prefix_stack_.emplace(node, 0);
        suf_str.append(node->str);
      } else {
        // prefix_str_.pop_back(node->str.length());
        prefix_str_len_ -= node->str.length();

        prefix_stack_.pop();
        if (prefix_stack_.empty())
          return false;
        last_prefix_semichar_ = prefix_stack_.top().first->str.length()
                                    ? prefix_stack_.top().first->str.back()
                                    : 0;
      }

      auto &[node_r, idx_r] = prefix_stack_.top();
      node = node_r;
      idx = &idx_r;
      if (node->is_lazy_node)
        break;
    }

    *prefix_len = prefix_str_len_ / kTranF;

    // auto qw = prefix_str_.length() ? prefix_str_[prefix_str_.length() - 1] :
    // 0; CHECK_EQ(qw, last_prefix_semichar_);

    // prefix_str_.append(suf_str);
    prefix_str_len_ += suf_str.length();

    auto curr_last = last_prefix_semichar_;
    if (suf_str.length()) {
      last_prefix_semichar_ = suf_str.back();
    }

    if (node->is_lazy_node) {
      *idx = kElementNum; // so that next time will pop this node
      handleLazyNode(node, suf_str);

      std::move(suf_str).toString(key, curr_last);
      key->append(lazy_keys_.back().first);
      *values = lazy_keys_.back().second;
      lazy_keys_.pop_back();
      return true;
    } else {
      std::move(suf_str).toString(key, curr_last);
    }

    *values = node->values.back();
    node->values.pop_back();
    return true;
  }

  auto valueNum() const { return trie_->value_num_; }

private:
  void handleLazyNode(Node *node, SemiString &suf_str) {
    DCHECK(lazy_keys_.empty());
    lazy_keys_.reserve(node->lazy_keys.size());
    auto pre_len = node->str.length();
    if (prefix_str_len_ % 2 == 1) {
      suf_str.pop_back(1);
      --pre_len;
    }
    // notice: pre_len might be 0
    std::string key;
    for (auto &lazy_key : node->lazy_keys) {
      int64_t key_i = (int64_t)lazy_key.prefix_len - pre_len;
      if (key_i <= 0) {
        std::move(SemiString(lazy_key.key.substr_tail(-key_i)))
            .toString(&key, 0);
        lazy_keys_.emplace_back(std::move(key), std::move(lazy_key.value));
      } else {
        DCHECK(key_i % 2 == 0);
        lazy_key.key.to_string(&key, 0);
        auto str = lazy_keys_.back().first.substr(0, key_i / kTranF) + key;
        lazy_keys_.emplace_back(std::move(str), std::move(lazy_key.value));
      }
    }
    std::sort(lazy_keys_.begin(), lazy_keys_.end(),
              [](auto &x, auto &y) { return x.first > y.first; });
  }

  std::unique_ptr<Trie<ValueT>> trie_;

  std::stack<std::pair<Node *, uint8_t>> prefix_stack_;
  // SemiString prefix_str_;
  size_t prefix_str_len_ = 0;
  uint8_t last_prefix_semichar_ = 0;

  std::vector<std::pair<std::string, ValueT>> lazy_keys_;
};

} // namespace whippet_sort::trie