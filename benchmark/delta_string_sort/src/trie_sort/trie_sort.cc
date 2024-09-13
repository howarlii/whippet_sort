#include "trie_sort.h"

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

namespace whippet_sort::trie {

namespace trie__internal {

SemiStringView::SemiStringView(const SemiString &str) {
  str_ = str.str_;
  is_first_half_ = str.is_first_half_;
  length_ = str.length_;
}

} // namespace trie__internal

} // namespace whippet_sort::trie