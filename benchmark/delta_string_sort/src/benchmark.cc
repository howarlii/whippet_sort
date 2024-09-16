#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <memory>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/encoding.h"
#include "parquet/file_reader.h"
#include "parquet/types.h"

#include "parquet_sorter.h"
#include "parquet_sorter_trie.h"
#include "utils.h"

using namespace whippet_sort;

int main(const int argc, const char *argv[]) {
  nice(-20);
  const int num_runs = 5;
  const uint32_t col_idx = 1;

  std::string input_file =
      std::string(PROJECT_SOURCE_DIR) + "/data/input-2e4-40.parquet";
  if (argc > 1) {
    input_file = std::string(argv[1]);
  }

  {
    // Benchmark Arrow sorting
    std::vector<std::function<std::string()>> steps;
    std::unique_ptr<whippet_sort::ParquetSorterArrow> sorter;
    steps.push_back([&]() {
      Utils::drop_file_cache(input_file);
      sorter = std::make_unique<whippet_sort::ParquetSorterArrow>(input_file,
                                                                  col_idx);
      return std::string("read");
    });
    steps.push_back([&]() {
      // sorter.print_column();
      auto idx_array = sorter->sort_by_column();
      return std::string("sort");
    });
    steps.push_back([&]() {
      sorter->generate_result();
      return "generate result";
    });
    auto [arrow_median, arrow_average] =
        Utils::benchmark("Arrow", num_runs, std::move(steps));

    std::cout << "# Arrow sorting - Median: " << arrow_median
              << "ms, Average: " << arrow_average << "ms" << std::endl;
  }

  {
    std::vector<std::function<std::string()>> steps;
    std::unique_ptr<whippet_sort::ParquetSorterTrieArrow> sorter;
    steps.push_back([&]() {
      Utils::drop_file_cache(input_file);
      sorter =
          std::make_unique<whippet_sort::ParquetSorterTrieArrow>(input_file, 0);
      sorter->read_all();
      return "read";
    });
    steps.push_back([&]() {
      auto idx_array = sorter->sort_by_column();
      return "sort";
    });
    steps.push_back([&]() {
      sorter->generate_result();
      return "generate result";
    });
    auto [arrow_median, arrow_average] =
        Utils::benchmark("Trie-Arrow", num_runs, std::move(steps));

    std::cout << "# Whippet sorting (Trie-Arrow) - Median: " << arrow_median
              << "ms, Average: " << arrow_average << "ms" << std::endl;
  }

  {
    std::vector<std::function<std::string()>> steps;
    std::unique_ptr<whippet_sort::ParquetSorterTrie> sorter;
    steps.push_back([&]() {
      Utils::drop_file_cache(input_file);
      sorter = std::make_unique<whippet_sort::ParquetSorterTrie>(input_file,
                                                                 col_idx);
      auto idx_array = sorter->sort_by_column();
      return "read+sort";
    });
    steps.push_back([&]() {
      sorter->generate_result();
      return "generate result";
    });
    auto [median, average] =
        Utils::benchmark("Trie", num_runs, std::move(steps));

    std::cout << "# Whippet sorting (Trie) - Median: " << median
              << "ms, Average: " << average << "ms" << std::endl;
  }
  // Check correctness
  // bool count_correct =
  //     check_whippet_sort_correctness("out_whippet_count.parquet", 0);
  // std::cout << "Count Base Whippet sort correctness: "
  //           << (count_correct ? "Correct" : "Incorrect") << std::endl;

  return 0;
}