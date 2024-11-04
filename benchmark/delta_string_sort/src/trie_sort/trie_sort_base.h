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

  auto get_insert_time_us() const { return insert_time_; }

protected:
  struct Timer {
    const std::chrono::time_point<std::chrono::system_clock> start_;
    TrieBuilderBase *base_;

    Timer(TrieBuilderBase *base)
        : start_(std::chrono::high_resolution_clock::now()), base_(base) {}

    ~Timer() { stop(); }

    void stop() {
      if (base_) {
        base_->insert_time_ +=
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_);
        base_->insert_time_cnt_++;
      }
      base_ = nullptr;
    }
  };

  std::chrono::microseconds insert_time_{0};
  size_t insert_time_cnt_;
};
} // namespace whippet_sort