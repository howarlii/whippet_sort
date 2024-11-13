#pragma once

#include <cerrno>
#include <chrono>
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

namespace whippet_sort {

class TrieBuilderBase {
public:
  TrieBuilderBase() = default;
  virtual ~TrieBuilderBase() = default;

  /**
   * @brief Insert a key into the tree
   * @param prefix_len The length of the prefix shared with last inserted key
   * @param key The suffix of the key to insert
   * @param value The value to the key
   */
  virtual void insert(size_t prefix_len, std::string_view key, int value) = 0;

  virtual void
  insert(std::vector<std::tuple<size_t, std::string_view, int>> keys) {
    Timer tc(this);
    for (auto &&[prefix_len, key, value] : keys) {
      insert(prefix_len, key, value);
    }
  }

  virtual size_t valueNum() const = 0;

  auto get_insert_time_ms() const { return insert_time_ms_; }

protected:
  struct Timer {
    struct timespec start_tm_;
    TrieBuilderBase *base_;

    Timer(TrieBuilderBase *base) : base_(base) {
      clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start_tm_);
    }

    ~Timer() { stop(); }

    void stop() {
      if (base_) {
        struct timespec end;
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end);
        base_->insert_time_ms_ += (end.tv_sec - start_tm_.tv_sec) * 1e3 +
                                  (end.tv_nsec - start_tm_.tv_nsec) / 1e6;
        base_->insert_time_cnt_++;
      }
      base_ = nullptr;
    }
  };

  double insert_time_ms_{0};
  size_t insert_time_cnt_;
};

} // namespace whippet_sort