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

#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/types.h>

namespace whippet_sort::trie {

using namespace trie__internal;

template <typename ValueT> struct Trie {
  struct Node {
    std::unique_ptr<Node> children[kElementNum]{nullptr};
    Node *parent{nullptr};
    SemiStringView str; // string on the edge to the parent
    size_t pdep{0};
    std::vector<ValueT> values;
  };

  std::unique_ptr<Node> root_{nullptr};
  size_t value_num_;

  std::deque<std::string> str_pool_; // TODO: to be optimized
  // std::deque<std::string> str_pool_; // slower, why?
};

class TriePrinter;

class DictTreeBuilder {
  using ValueT = int;

  using Node = Trie<ValueT>::Node;
  friend class TriePrinter;

public:
  DictTreeBuilder() : trie_(std::make_unique<Trie<ValueT>>()) { reset(); };

  /**
   * @brief Insert a key into the tree
   * @param prefix_len The length of the prefix shared with last inserted key
   * @param key The suffix of the key to insert
   * @param value The value to the key
   * @return The non-prefix length of the key
   */
  size_t Insert(size_t prefix_len, const std::string_view &key_in,
                ValueT value) {
    ++trie_->value_num_;
    trie_->str_pool_.emplace_back(key_in);
    SemiStringView key(key_in);

    prefix_len *= kTranF;
    CHECK_LE(prefix_len, curr_length_)
        << "prefix_len too large! prefix_len: " << prefix_len
        << ", curr_length_: " << curr_length_;
    if (prefix_len == 0) {
      curr_node_ = trie_->root_.get();
      curr_length_ = 0;
    } else {
      while (curr_node_->pdep >= prefix_len) {
        curr_length_ = curr_node_->pdep;
        curr_node_ = curr_node_->parent;
      }
    }

    size_t key_i = 0;
    while (1) {
      // Go to a Node that has a prefix with the key
      if (curr_length_ > prefix_len) {
        auto curr_skip_pref_len =
            (prefix_len > curr_node_->pdep) ? prefix_len - curr_node_->pdep : 0;
        size_t same_len;
        if (curr_skip_pref_len) {
          same_len = key.substr_tail(key_i).prefix_len(
              curr_node_->str.substr_tail(curr_skip_pref_len));
        } else {
          same_len = key.substr_tail(key_i).prefix_len(curr_node_->str);
        }

        key_i += same_len;
        if (curr_skip_pref_len + same_len < curr_node_->str.length()) {
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

      // Here we on the node curr_node_ that has a prefix with the key
      if (key_i == key.length()) {
        curr_node_->values.push_back(value);
        return 0;
      } else if (curr_node_->children[key[key_i]] == nullptr) {
        auto new_node_u = std::make_unique<Node>();
        auto new_node = new_node_u.get();
        new_node->str = key.substr_tail(key_i);
        new_node->parent = curr_node_;
        new_node->pdep = curr_length_;
        new_node->values.push_back(value);
        curr_node_->children[key[key_i]] = std::move(new_node_u);

        curr_node_ = new_node;
        curr_length_ += new_node->str.length();
        return (new_node->str.length() + 1) / kTranF;
      } else {
        curr_node_ = curr_node_->children[key[key_i]].get();
        curr_length_ += curr_node_->str.length();
      }
    }
    DCHECK(false) << "should not reach here";
  }

  auto valueNum() const { return trie_->value_num_; }

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
  };

  std::unique_ptr<Trie<ValueT>> trie_;

  Node *curr_node_;
  size_t curr_length_;
};

class TriePrinter {
  using ValueT = int;

  friend class DictTreeBuilder;

  using Node = DictTreeBuilder::Node;

public:
  TriePrinter(std::unique_ptr<Trie<ValueT>> &&trie) : trie_(std::move(trie)) {
    prefix_stack_.emplace(trie_->root_.get(), 0);
  }

  bool has_next() const { return !prefix_stack_.empty(); }

  bool Next(size_t *prefix_len, std::string *key, int *values) {
    if (!has_next())
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
        prefix_str_len -= node->str.length();

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
    }

    *prefix_len = prefix_str_len / kTranF;

    // auto qw = prefix_str_.length() ? prefix_str_[prefix_str_.length() - 1] :
    // 0; CHECK_EQ(qw, last_prefix_semichar_);

    // prefix_str_.append(suf_str);
    prefix_str_len += suf_str.length();

    auto t = last_prefix_semichar_;
    if (suf_str.length()) {
      last_prefix_semichar_ = suf_str.back();
    }
    std::move(suf_str).toString(key, t);

    *values = node->values.back();
    node->values.pop_back();
    return true;
  }
  auto valueNum() const { return trie_->value_num_; }

private:
  std::unique_ptr<Trie<ValueT>> trie_;

  std::stack<std::pair<Node *, uint8_t>> prefix_stack_;
  // SemiString prefix_str_;
  size_t prefix_str_len = 0;
  uint8_t last_prefix_semichar_ = 0;
};

} // namespace whippet_sort::trie