#pragma once
#include <algorithm>
#include <any>
#include <bit>
#include <cmath>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace whippet_sort::stitch {

#define BIG_LITTLE_SWAP32(x)                                                   \
  ((((*(long int *)&(x)) & 0xff000000) >> 24) |                                \
   (((*(long int *)&(x)) & 0x00ff0000) >> 8) |                                 \
   (((*(long int *)&(x)) & 0x0000ff00) << 8) |                                 \
   (((*(long int *)&(x)) & 0x000000ff) << 24))

template <size_t WIDTH> struct StitchingT {
  size_t idx;
  uint8_t v[WIDTH];

  std::strong_ordering operator<=>(const StitchingT &rhs) const {
    return std::memcmp(v, rhs.v, WIDTH) <=> 0;
  }
};

template <size_t WIDTH> class StitchingSorter {
public:
  using ValueT = StitchingT<WIDTH>;
  // static constexpr size_t WIDTH = sizeof(ValueT); // in byte

  StitchingSorter(size_t num_rows, ValueT *data)
      : num_rows_(num_rows), data_(data) {}

  ~StitchingSorter() {}

  void sort() { std::sort(data_, data_ + num_rows_); }

  void grouping(std::vector<std::pair<size_t, size_t>> *out, size_t *sorted_idx,
                size_t offset) {
    auto v = data_[0];
    sorted_idx[0] = data_[0].idx;
    size_t group_begin = 0;
    for (size_t i = 1; i < num_rows_; i++) {
      sorted_idx[i] = data_[i].idx;
      auto t = (v <=> data_[i]);
      if (t < 0) {
        if (group_begin != i - 1)
          out->emplace_back(group_begin + offset, i + offset);
        v = data_[i];
        group_begin = i;
      } else if (t > 0) {
        DLOG(FATAL) << "Unsorted data";
      }
    }
    if (group_begin != num_rows_ - 1)
      out->emplace_back(group_begin + offset, num_rows_ + offset);
  }

private:
  size_t num_rows_{0};
  ValueT *data_;
};

class StitchingSorterOperator {
public:
  StitchingSorterOperator() {}

  virtual ~StitchingSorterOperator() {}

  virtual void init(size_t num_rows, std::vector<size_t> *idx,
                    std::vector<std::pair<size_t, size_t>> last_grouping) {
    num_rows_ = num_rows;
    idx_ = idx;
    last_grouping_ = std::move(last_grouping);
    valid_rows_ = 0;
    for (auto [l, r] : last_grouping_) {
      valid_rows_ += r - l;
      CHECK(l < r);
    }
  }

  virtual void setData(const std::vector<uint32_t> &a) = 0;

  virtual void sort() = 0;

  virtual std::vector<std::pair<size_t, size_t>> grouping() = 0;

  auto valid_rows() const { return valid_rows_; }

protected:
  size_t num_rows_, valid_rows_;
  std::vector<size_t> *idx_;
  std::vector<std::pair<size_t, size_t>> last_grouping_;
};

template <size_t WIDTH>
class StitchingSorterOperatorImpl : public StitchingSorterOperator {
public:
  StitchingSorterOperatorImpl() {}

  void init(size_t num_rows, std::vector<size_t> *idx,
            std::vector<std::pair<size_t, size_t>> last_grouping) override {
    StitchingSorterOperator::init(num_rows, idx, last_grouping);
    data_.resize(num_rows);
  }

  void setData(const std::vector<uint32_t> &a) override { setDataImpl(a); }

  void sort() override {
    CHECK_EQ(filled_width_, WIDTH);
    sorters_.reserve(last_grouping_.size());
    for (auto [l, r] : last_grouping_) {
      auto s =
          sorters_.emplace_back(l, StitchingSorter<WIDTH>{r - l, &data_[l]});
      s.second.sort();
    }
  }

  std::vector<std::pair<size_t, size_t>> grouping() override {
    std::vector<std::pair<size_t, size_t>> new_grouping;
    new_grouping.reserve(sorters_.size() * 2);
    for (auto &[l, s] : sorters_) {
      s.grouping(&new_grouping, &(*idx_)[l], l);
    }
    return new_grouping;
  }

private:
  template <typename T> void setDataImpl(const std::vector<T> &a) {
    static_assert(std::endian::native == std::endian::little);
    static_assert(sizeof(T) == 4);
    CHECK_EQ(a.size(), num_rows_);
    CHECK_GE(WIDTH - filled_width_, sizeof(T));

    if (filled_width_ == 0) {
      for (auto [l, r] : last_grouping_) {
        for (size_t i = l; i < r; i++) {
          data_[i].idx = (*idx_)[i];
        }
      }
    }

    for (auto [l, r] : last_grouping_) {
      for (size_t i = l; i < r; i++) {
        *(reinterpret_cast<T *>(data_[i].v + filled_width_)) =
            BIG_LITTLE_SWAP32(a[(*idx_)[i]]);
      }
    }
    filled_width_ += sizeof(T);
  }

  std::vector<StitchingT<WIDTH>> data_;
  size_t filled_width_{0};
  std::vector<std::pair<size_t, StitchingSorter<WIDTH>>> sorters_;
};

inline std::unique_ptr<StitchingSorterOperator>
createStitchingSorterOperator(size_t width) {
  const auto kMaxWidth = 32;
  auto GeneJ = []<size_t... M>(std::index_sequence<M...>) constexpr {
    return std::array<std::function<std::unique_ptr<StitchingSorterOperator>()>,
                      sizeof...(M)>{
        []() { return std::make_unique<StitchingSorterOperatorImpl<M>>(); }...};
  };
  const auto create_array = GeneJ(std::make_index_sequence<kMaxWidth>{});

  if (width < kMaxWidth) {
    return create_array[width]();
  }
  DLOG(FATAL) << "Unsupported width: " << width;
  return nullptr;
}

} // namespace whippet_sort::stitch
