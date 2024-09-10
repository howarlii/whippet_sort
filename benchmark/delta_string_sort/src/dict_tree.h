#pragma once

#include <cerrno>
#include <cmath>
#include <cstddef>
#include <deque>
#include <memory>
#include <stack>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/types.h>

namespace whippet_sort {

namespace dict_tree_internal {

constexpr static uint8_t kElementBit = 4;
constexpr static uint8_t kElementNum = 1 << kElementBit;
constexpr static size_t kTranF = sizeof(char) * 8 / kElementBit;
constexpr static uint8_t kMask0 = 0x0f;
constexpr static uint8_t kMask1 = 0xf0;

static_assert(sizeof(char) * 8 % kElementBit == 0, " ");

class SemiString;
class SemiStringView {
  friend class SemiString;

public:
  SemiStringView() = default;
  SemiStringView(const std::string_view &str)
      : str_(str), length_(str_.length() * 2) {}

  SemiStringView(const SemiString &str);

  uint8_t operator[](size_t i) const {
    CHECK_LT(i, length_);
    if (is_first_half_) {
      ++i;
    }

    return (i & 1) ? (str_[i / 2] & kMask1 >> kElementBit)
                   : (str_[i / 2] & kMask0);
  }

  size_t length() const { return length_; }

  SemiStringView substr(size_t pos, size_t len) const {
    CHECK_LE(pos + len, length_);
    if (is_first_half_)
      pos++;

    auto start = pos / 2;
    auto end = (pos + len - 1) / 2;
    CHECK_LT(end, str_.length());
    SemiStringView ret;
    ret.str_ = str_.substr(start, end - start + 1);
    ret.is_first_half_ = pos & 1;
    ret.length_ = len;

    return ret;
  }

  SemiStringView substr_tail(size_t start_pos) const {
    return substr(start_pos, length() - start_pos);
  }

  size_t prefix_len(const SemiStringView &rhs) const {
    CHECK_EQ(is_first_half_, rhs.is_first_half_);
    size_t i = 0;
    for (; i < length() && i < rhs.length(); ++i) {
      if ((*this)[i] != rhs[i]) {
        return i;
      }
    }
    return i;
    // TODO optimize it
    // if (is_first_half_) {
    //   if ((str_[0] & kMask[1]) != (rhs.str_[0] & kMask[1])) {
    //     return 0;
    //   }
    //   i = 1;
    // }
    // for (; i < lhs.size() && i < rhs.size(); ++i) {
    //   if (lhs[i] != rhs[i]) {
    //     break;
    //   }
    // }
    // auto ans = std::max(i * kTranF - fist_is_half, max_len);
    // if (ans < max_len && (lhs[i] & kMask0) == (rhs[i] & kMask0)) {
    //   ++ans;
    // }
  }

private:
  std::string_view str_;
  bool is_first_half_ = false;
  size_t length_;
};

class SemiString {
  friend class SemiStringView;

public:
  SemiString() = default;

  uint8_t operator[](size_t i) const {
    CHECK_LT(i, length_);
    if (is_first_half_) {
      ++i;
    }

    return (i & 1) ? (str_[i / 2] & kMask1 >> kElementBit)
                   : (str_[i / 2] & kMask0);
  }

  void set(size_t pos, uint8_t v) {
    if (is_first_half_) {
      ++pos;
    }
    CHECK_LT(pos, 1 + 2 * str_.size());
    if (pos & 1) {
      str_[pos / 2] = (str_[pos / 2] & kMask0) | (v << kElementBit);
    } else {
      str_[pos / 2] = (str_[pos / 2] & kMask1) | v;
    }
  }

  size_t length() const { return length_; }

  void append(const SemiStringView &v) {
    if (length_ == 0) {
      str_ = v.str_;
      is_first_half_ = v.is_first_half_;
      length_ = v.length_;
      return;
    }

    CHECK_EQ((is_first_half_ + length_) % 2, v.is_first_half_);

    if (v.is_first_half_) {
      set(length_, v[0] & kMask1 >> kElementBit);
      str_.append(v.str_.substr(1));
    } else {
      str_.append(v.str_);
    }
    length_ += v.length_;
  }

  void pop_back(size_t len) {
    CHECK_GE(length_, len);
    length_ -= len;
    if (length_ == 0) {
      str_.clear();
    } else {
      str_.resize((is_first_half_ + length_ + 1) / 2);
    }
  }

  void toString(std::string *ret, uint8_t first_elm) const && {
    CHECK((is_first_half_ + length_) % 2 == 0);
    CHECK(first_elm < kElementNum);
    *ret = std::move(str_);
    if (is_first_half_) {
      (*ret)[0] = first_elm + ((*ret)[0] & kMask1);
    }
  }

private:
  std::string str_;
  bool is_first_half_ = false;
  size_t length_ = 0;
};

} // namespace dict_tree_internal

using namespace dict_tree_internal;

class DictTreePrinter;

class DictTreeBuilder {
  using ValueT = int;

  friend class DictTreePrinter;

  struct Node {
    std::unique_ptr<Node> children[kElementNum]{nullptr};
    Node *parent{nullptr};
    SemiStringView str; // string on the edge to the parent
    // size_t length{0};
    size_t pdep{0};
    std::vector<ValueT> values;
  };

  std::deque<std::string> str_pool_; // TODO: to be optimized

public:
  DictTreeBuilder() {
    root_ = std::make_unique<Node>();
    root_->pdep = 0;
    curr_node_ = root_.get();
    curr_length_ = 0;
  };

  /**
   * @brief Insert a key into the tree
   * @param prefix_len The length of the prefix shared with last inserted key
   * @param key The suffix of the key to insert
   * @param value The value to the key
   * @return The non-prefix length of the key
   */
  size_t Insert(size_t prefix_len, const std::string_view &key_in,
                ValueT value) {
    str_pool_.emplace_back(key_in);
    SemiStringView key(key_in);

    prefix_len *= kTranF;
    CHECK_LE(prefix_len, curr_length_);
    if (prefix_len == 0) {
      curr_node_ = root_.get();
      curr_length_ = 0;
    } else {
      while (curr_node_->pdep >= prefix_len) {
        curr_length_ = curr_node_->pdep;
        curr_node_ = curr_node_->parent;
      }
    }
    if (key_in.length() == 0) {
      curr_node_->values.push_back(value);
      return 0;
    }

    size_t key_i = 0;
    while (1) {
      // Go to a Node that has a prefix with the key
      if (curr_length_ > prefix_len) {
        auto curr_skip_pref_len =
            (prefix_len > curr_node_->pdep) ? prefix_len - curr_node_->pdep : 0;
        auto same_len = key.substr_tail(key_i).prefix_len(
            curr_node_->str.substr_tail(curr_skip_pref_len));

        key_i = same_len;
        if (curr_skip_pref_len + same_len < curr_node_->str.length()) {
          auto new_node_u = std::make_unique<Node>();
          auto new_node = new_node_u.get();
          new_node->str =
              curr_node_->str.substr(0, curr_skip_pref_len + same_len);
          // new_node->length = new_node->str.length();
          new_node->parent = curr_node_->parent;
          new_node->pdep = curr_node_->pdep;

          curr_node_->str =
              curr_node_->str.substr_tail(curr_skip_pref_len + same_len);
          // curr_node_->length = curr_node_->str.length();
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

  std::unique_ptr<DictTreePrinter> build();

private:
  std::unique_ptr<Node> root_;

  Node *curr_node_;
  size_t curr_length_;
};

class DictTreePrinter {
  friend class DictTreeBuilder;

  using Node = DictTreeBuilder::Node;

  DictTreePrinter(std::unique_ptr<Node> node) : root_(std::move(node)) {
    prefix_stack_.emplace(root_.get(), 0);
  }

public:
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
        prefix_str_.pop_back(node->str.length());
        prefix_stack_.pop();
        if (prefix_stack_.empty())
          return false;
      }

      auto &[node_r, idx_r] = prefix_stack_.top();
      node = node_r;
      idx = &idx_r;
    }

    *prefix_len = prefix_str_.length() / kTranF;
    auto last_elm =
        prefix_str_.length() ? prefix_str_[prefix_str_.length() - 1] : 0;
    prefix_str_.append(suf_str);
    std::move(suf_str).toString(key, last_elm);

    *values = node->values.back();
    node->values.pop_back();
    return true;
  }

private:
  std::unique_ptr<Node> root_ = nullptr;

  std::stack<std::pair<Node *, uint8_t>> prefix_stack_;
  SemiString prefix_str_;
};

} // namespace whippet_sort