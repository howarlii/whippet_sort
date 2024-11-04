#include <functional>
#include <memory>
#include <random>

#include "parquet_sorter.h"
#include "parquet_sorter_trie.h"
#include "parquet_sorter_trie_v2.h"
#include "utils.h"

#include "arrow/result.h"
#include "arrow/status.h"
#include "gtest/gtest.h"

namespace whippet_sort {
namespace {

class ParquetSort : public ::testing::Test {
public:
  static const std::string input_file;
  static const uint32_t col_idx;
  static size_t std_hash;

  static void SetUpTestSuite() {
    whippet_sort::ParquetSorterArrow sorter(input_file, col_idx);
    sorter.read_all();
    // sorter.print_column();
    sorter.sort_by_column();
    sorter.generate_result();
    // DLOG(INFO) << sorter.get_sorted_column()->ToString();

    std_hash = sorter.compute_hash();
    LOG(INFO) << "hash: " << std_hash;
  }
};
const std::string ParquetSort::input_file =
    std::string(PROJECT_SOURCE_DIR) + "/data/input-ty2-2e6-800.parquet";
const uint32_t ParquetSort::col_idx = 1;
size_t ParquetSort::std_hash = 0;

TEST_F(ParquetSort, Hacked) {
  whippet_sort::ParquetSorterHacked sorter(input_file, col_idx);

  sorter.read_all();
  sorter.sort_by_column();
  // DLOG(INFO) << sorter.get_sorted_column()->ToString();

  auto hash = sorter.compute_hash();
  ASSERT_EQ(hash, std_hash);
}

TEST_F(ParquetSort, Trie) {
  whippet_sort::ParquetSorterTrie sorter(input_file, col_idx);

  sorter.sort_by_column();
  sorter.pre_sort();
  sorter.print_trie();
  sorter.generate_result();
  // DLOG(INFO) << sorter.get_sorted_column()->ToString();

  auto hash = sorter.compute_hash();
  ASSERT_EQ(hash, std_hash);
}

TEST_F(ParquetSort, TrieV2) {
  whippet_sort::ParquetSorterTrieV2 sorter(input_file, col_idx);
  sorter.set_trie_builder(std::make_unique<trie_v2::TrieBuilder>());

  sorter.sort_by_column();
  sorter.pre_sort();
  sorter.print_trie();
  sorter.generate_result();
  // DLOG(INFO) << sorter.get_sorted_column()->ToString();

  auto hash = sorter.compute_hash();
  ASSERT_EQ(hash, std_hash);
}

TEST_F(ParquetSort, TrieV2BFS) {
  whippet_sort::ParquetSorterTrieV2 sorter(input_file, col_idx);
  sorter.set_trie_builder(std::make_unique<trie_v2::TrieBuilderBfs>());
  sorter.sort_by_column();
  sorter.pre_sort();
  sorter.print_trie();
  sorter.generate_result();
  // DLOG(INFO) << sorter.get_sorted_column()->ToString();

  auto hash = sorter.compute_hash();
  ASSERT_EQ(hash, std_hash);
}

} // namespace
} // namespace whippet_sort