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

// template <typename ValueT>
std::unique_ptr<Trie<int>::Node> TrieBuilder::createNewNode(SemiStringView key,
                                                            ValueT &&value) {
  auto new_node = std::make_unique<Node>();

  new_node->parent = curr_node_;
  new_node->pdep = curr_length_;
  if (curr_depth_ > config_.lazy_dep_lmt) {
    new_node->is_lazy_node = true;
    new_node->lazy_keys.emplace_back(
        Trie<ValueT>::LazyKey{.prefix_len = 0, .key = key, .value = value});
    constexpr auto kSemiInplaceLen = 32;
    new_node->str =
        key.length() > kSemiInplaceLen ? key.substr(0, kSemiInplaceLen) : key;
    // TODO: hard code kSemiInplaceLen here, might be align with
    // kSemiInplaceLen
  } else {
    new_node->is_lazy_node = false;
    new_node->str = key;
    new_node->values.push_back(value);
  }
  return new_node;
}

} // namespace whippet_sort::trie