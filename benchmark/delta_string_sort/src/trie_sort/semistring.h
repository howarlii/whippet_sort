#pragma once

#include <cerrno>
#include <cmath>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/types.h>

namespace whippet_sort::trie ::trie__internal {

constexpr static int kInplaceStore = 2;

constexpr static uint8_t kElementBit = 4;
constexpr static uint8_t kElementNum = 1 << kElementBit;
constexpr static size_t kTranF = sizeof(char) * 8 / kElementBit;
constexpr static uint8_t kMask0 = 0xf0;
constexpr static uint8_t kMask1 = 0x0f;

static inline uint8_t get_mask1(uint8_t v) { return (v & kMask1); }
static inline uint8_t set_mask1(uint8_t v) {
  // DCHECK_LT(v, kElementNum);
  return (v);
}
static inline uint8_t get_mask0(uint8_t v) {
  return (v & kMask0) >> kElementBit;
}
static inline uint8_t set_mask0(uint8_t v) {
  // DCHECK_LT(v, kElementNum);
  return (v) << kElementBit;
}

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

    return (i & 1) ? get_mask1(str_[i >> 1]) : get_mask0(str_[i >> 1]);
  }

  inline uint8_t back() const {
    CHECK_GT(length_, 0);
    return (*this)[length_ - 1];
  }

  inline size_t length() const { return length_; }

  SemiStringView substr(size_t pos, size_t len) const {
    DCHECK_LE(pos + len, length_);
    if (length_ == 0) {
      return SemiStringView();
    }
    if (len == length_)
      return *this;
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
    if (start_pos == 0)
      return *this;
    return substr(start_pos, length() - start_pos);
  }

  size_t prefix_eq_len(const SemiStringView &rhs) const {
    DCHECK_EQ(is_first_half_, rhs.is_first_half_);
    size_t i = 0;
    if (is_first_half_) {
      if (get_mask1(str_[0]) != get_mask1(rhs.str_[0])) {
        return 0;
      }
      i = 1;
    }

    // if (!inplace_str_enabled_) {
    //   load_inplace_str();
    // }
    // if (!rhs.inplace_str_enabled_) {
    //   rhs.load_inplace_str();
    // }

    using CmpT = size_t;
    constexpr auto gap = sizeof(CmpT) / sizeof(uint8_t) * kTranF;
    for (; i + gap < length() && i + gap < rhs.length(); i += gap) {
      auto p1 = *reinterpret_cast<const CmpT *>(str_.data() + (i + 1) / 2);
      auto p2 = *reinterpret_cast<const CmpT *>(rhs.str_.data() + (i + 1) / 2);
      if (auto t = p1 ^ p2) {
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

  void to_string(std::string *ret, uint8_t first_elm) const {
    CHECK((is_first_half_ + length_) % 2 == 0);
    CHECK(first_elm < kElementNum);
    *ret = std::move(str_);
    if (is_first_half_) {
      (*ret)[0] = set_mask0(first_elm) | set_mask1(get_mask1((*ret)[0]));
    }
  }

private:
  // void load_inplace_str() {
  //   inplace_str_enabled_ = true;

  //   static_assert(sizeof(inplace_str_[0]) / sizeof(uint8_t) == 8);

  //   constexpr auto gap = sizeof(uint64_t) / sizeof(uint8_t) * kTranF;
  //   auto s_ptr = str_.data() + is_first_half_;
  //   size_t i = 0;
  //   for (; i * gap + gap < length() && i < kInplaceStore; ++i) {
  //     inplace_str_[i] = *reinterpret_cast<const uint64_t *>(s_ptr + i * 8);
  //   }

  //   if (i < kInplaceStore && i * gap < length()) {
  //     inplace_str_[i] = 0;
  //     for (size_t j = i * 8; j * kTranF < length(); ++j) {
  //       inplace_str_[i] = (inplace_str_[i] << kElementBit) | s_ptr[j];
  //     }
  //   }
  // }

  std::string_view str_;
  bool is_first_half_ = false;
  size_t length_;
};

class SemiString {
  friend class SemiStringView;

public:
  SemiString() = default;

  SemiString(const SemiStringView &s) {
    str_ = s.str_;
    is_first_half_ = s.is_first_half_;
    length_ = s.length_;
    if (is_first_half_) {
      str_[0] = (str_[0] & kMask1);
    }
    if ((is_first_half_ + length_) & 1) {
      str_.back() = (str_.back() & kMask0);
    }
  }

  uint8_t operator[](size_t i) const {
    CHECK_GT(length_, 0);
    CHECK_LT(i, length_);
    if (is_first_half_) {
      ++i;
    }

    return (i & 1) ? get_mask1(str_[i >> 1]) : get_mask0(str_[i >> 1]);
  }

  void reserve(size_t len) { str_.reserve(len / 2 + 1); }

  inline uint8_t back() const {
    CHECK_GT(length_, 0);
    return (*this)[length_ - 1];
  }

  void set(size_t pos, uint8_t v) {
    if (is_first_half_) {
      ++pos;
    }
    DCHECK_LT(pos, 1 + 2 * str_.size());
    DCHECK_LT(v, kElementNum);
    if (pos & 1) {
      str_[pos / 2] = (str_[pos / 2] & kMask0) | set_mask1(v);
    } else {
      str_[pos / 2] = (str_[pos / 2] & kMask1) | set_mask0(v);
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

  SemiString substr(size_t pos, size_t len) const {
    CHECK_LE(pos + len, length());
    SemiStringView v(*this);
    return v.substr(pos, len);
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
      (*ret)[0] = set_mask0(first_elm) | set_mask1(get_mask1((*ret)[0]));
    }
  }

  bool operator>(const SemiString &rhs) const {
    CHECK_EQ(is_first_half_, rhs.is_first_half_);
    return str_ > rhs.str_;
  }

private:
  std::string str_;
  bool is_first_half_ = false;
  size_t length_ = 0;
};

} // namespace whippet_sort::trie::trie__internal