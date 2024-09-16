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

namespace whippet_sort::trie ::trie__internal {

constexpr static uint8_t kElementBit = 4;
constexpr static uint8_t kElementNum = 1 << kElementBit;
constexpr static size_t kTranF = sizeof(char) * 8 / kElementBit;
constexpr static uint8_t kMask0 = 0xf0;
constexpr static uint8_t kMask1 = 0x0f;

static_assert(sizeof(char) * 8 % kElementBit == 0, " ");

class SemiString;
class SemiStringView {
  friend class SemiString;

public:
  SemiStringView() = default;
  SemiStringView(const std::string_view &str)
      : str_(str), length_(str_.length() * kTranF) {}

  SemiStringView(const SemiString &str);

  uint8_t operator[](size_t i) const {
    DCHECK_GT(length_, 0);
    DCHECK_LT(i, length_);
    if (is_first_half_) {
      ++i;
    }

    return (i & 1) ? ((str_[i >> 1] & kMask1))
                   : ((str_[i >> 1] & kMask0) >> kElementBit);
  }

  inline uint8_t back() const {
    CHECK_GT(length_, 0);
    return (*this)[length_ - 1];
  }

  inline size_t length() const { return length_; }

  SemiStringView substr(size_t pos, size_t len) const {
    if (length_ == 0) {
      return SemiStringView();
    }
    DCHECK_LE(pos + len, length_);
    if (is_first_half_)
      pos++;

    auto start = pos >> 1;
    auto end = (pos + len - 1) >> 1;
    DCHECK_LT(end, str_.length());
    SemiStringView ret;
    ret.str_ = str_.substr(start, end - start + 1);
    ret.is_first_half_ = pos & 1;
    ret.length_ = len;

    return ret;
  }

  inline SemiStringView substr_tail(size_t start_pos) const {
    return substr(start_pos, length() - start_pos);
  }

  size_t prefix_len(const SemiStringView &rhs) const {
    CHECK_EQ(is_first_half_, rhs.is_first_half_);
    size_t i = 0;
    if (is_first_half_) {
      if (str_[0] != rhs.str_[0]) {
        return 0;
      }
      i = 1;
    }

    using CmpT = uint64_t;
    constexpr auto gap = sizeof(CmpT) / sizeof(uint8_t) * kTranF;
    for (; i + gap < length() && i + gap < rhs.length(); i += gap) {
      auto p1 = reinterpret_cast<const CmpT *>(str_.data() + (i + 1) / 2);
      auto p2 = reinterpret_cast<const CmpT *>(rhs.str_.data() + (i + 1) / 2);
      if (*p1 != *p2) {
        break;
      }
    }

    for (; i < length() && i < rhs.length(); ++i) {
      if ((*this)[i] != rhs[i]) {
        return i;
      }
    }
    return i;
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
    CHECK_GT(length_, 0);
    CHECK_LT(i, length_);
    if (is_first_half_) {
      ++i;
    }

    return (i & 1) ? ((str_[i >> 1] & kMask1))
                   : ((str_[i >> 1] & kMask0) >> kElementBit);
  }

  inline uint8_t back() const {
    CHECK_GT(length_, 0);
    return (*this)[length_ - 1];
  }

  void set(size_t pos, uint8_t v) {
    if (is_first_half_) {
      ++pos;
    }
    CHECK_LT(pos, 1 + 2 * str_.size());
    CHECK_LE(v, kMask1);
    if (pos & 1) {
      str_[pos / 2] = (str_[pos / 2] & kMask0) | v;
    } else {
      str_[pos / 2] = (str_[pos / 2] & kMask1) | (v << kElementBit);
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

    CHECK_EQ((is_first_half_ + length_) & 1, v.is_first_half_);

    if (v.is_first_half_) {
      set(length_, v[0]);
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
      (*ret)[0] = (first_elm << kElementBit) + ((*ret)[0] & kMask1);
    }
  }

private:
  std::string str_;
  bool is_first_half_ = false;
  size_t length_ = 0;
};

} // namespace whippet_sort::trie::trie__internal