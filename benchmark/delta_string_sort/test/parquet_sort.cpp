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
      std::string(PROJECT_SOURCE_DIR) + "/data/input.parquet";
  const uint32_t col_idx = 0;

  size_t hash = 0;
};

TEST_F(ParquetSort, arrow) {
  whippet_sort::ParquetSorterArrow sorter(input_file, col_idx);
  sorter.print_column();
  sorter.sort_by_column();
  auto ret = sorter.reorder_result();
  this->hash = sorter.compute_hash();
  LOG(INFO) << "hash: " << hash;

  // enable_debug = true;
}

TEST_F(ParquetSort, hack) {
  whippet_sort::ParquetSorterTrie sorter(input_file, col_idx);

  sorter.sort_by_column();
  auto ret = sorter.reorder_result();
  this->hash = sorter.compute_hash();
  LOG(INFO) << "hash: " << hash;

  // enable_debug = true;
}

} // namespace
} // namespace whippet_sort