#include <functional>
#include <memory>
#include <random>

#include "parquet_sorter.h"
#include "parquet_sorter_trie.h"
#include "utils.h"

#include "arrow/result.h"
#include "arrow/status.h"
#include "gtest/gtest.h"

namespace whippet_sort {
namespace {

class ParquetSort : public ::testing::Test {
public:
  const std::string input_file =
      std::string(PROJECT_SOURCE_DIR) + "/data/input-2e4-40.parquet";
  const uint32_t col_idx = 1;

  const size_t std_hash = 5783210521191338985ULL;
};

TEST_F(ParquetSort, arrow) {
  whippet_sort::ParquetSorterArrow sorter(input_file, col_idx);
  sorter.print_column();
  sorter.sort_by_column();
  sorter.generate_result();
  auto hash = sorter.compute_hash();
  ASSERT_EQ(hash, std_hash);
  LOG(INFO) << "hash: " << hash;

  // enable_debug = true;
}

TEST_F(ParquetSort, str_col) {
  whippet_sort::ParquetSorterTrieArrow sorter(input_file, 0);

  sorter.read_all();
  sorter.sort_by_column();
  sorter.generate_result();
  auto hash = sorter.compute_hash();
  ASSERT_EQ(hash, std_hash);
  LOG(INFO) << "hash: " << hash;

  // enable_debug = true;
}

TEST_F(ParquetSort, str_col2) {
  whippet_sort::ParquetSorterTrie sorter(input_file, col_idx);

  sorter.sort_by_column();
  sorter.generate_result();
  auto hash = sorter.compute_hash();
  ASSERT_EQ(hash, std_hash);
  LOG(INFO) << "hash: " << std_hash;

  // enable_debug = true;
}

} // namespace
} // namespace whippet_sort