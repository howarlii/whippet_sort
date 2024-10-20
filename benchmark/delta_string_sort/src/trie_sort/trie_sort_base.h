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
  virtual void insert(size_t prefix_len, const std::string_view &key,
                      int value) = 0;

  virtual size_t valueNum() const = 0;
};
} // namespace whippet_sort