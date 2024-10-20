#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <gflags/gflags.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "parquet_sorter.h"
#include "parquet_sorter_trie.h"
#include "parquet_sorter_trie_v2.h"
#include "utils.h"

using namespace whippet_sort;

DEFINE_string(input_file,
              std::string(PROJECT_SOURCE_DIR) + "/data/input-2e5-100.parquet",
              "Input file path");
DEFINE_int32(sort_col_idx, 1, "Column index to sort by");

DEFINE_bool(hi_arrow, false, "Run high-level Arrow sorting benchmark");
DEFINE_bool(low_arrow, false, "Run low-level Arrow sorting benchmark");
DEFINE_bool(trie, false, "Run trie-based sorting benchmark");
DEFINE_bool(trie_v2, false, "Run trie-based sorting benchmark v2");

DEFINE_int32(trie_lazy_dep_lmt, 5, "Trie lazy depth limit");
DEFINE_int32(trie_lazy_key_burst_lmt, 2048, "Trie lazy key burst limit");

int main(int argc, char *argv[]) {
  // Parse command line flags
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  nice(-20);
  const int num_runs = 5;
  const uint32_t col_idx = FLAGS_sort_col_idx;

  // Use the input_file flag
  std::string input_file = FLAGS_input_file;

  // Check if any flags were set, if not, run all benchmarks
  bool run_all = !FLAGS_hi_arrow && !FLAGS_low_arrow && !FLAGS_trie;
  if (FLAGS_hi_arrow || run_all) {
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
        Utils::benchmark("hi-Arrow", num_runs, std::move(steps));

    std::cout << "# hi-Arrow sorting - Median: " << arrow_median
              << "ms, Average: " << arrow_average << "ms" << std::endl;
  }

  if (FLAGS_low_arrow || run_all) {
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
#ifndef NDEBUG
      sorter->check_correctness();
#endif
      return "generate result";
    });
    auto [arrow_median, arrow_average] =
        Utils::benchmark("low-Arrow", num_runs, std::move(steps));

    std::cout << "# Whippet sorting (low-Arrow) - Median: " << arrow_median
              << "ms, Average: " << arrow_average << "ms" << std::endl;
  }

  if (FLAGS_trie || run_all) {
    std::unique_ptr<whippet_sort::ParquetSorterTrie> sorter;
    trie::TrieConfig config;
    config.lazy_dep_lmt = FLAGS_trie_lazy_dep_lmt;
    config.lazy_key_burst_lmt = FLAGS_trie_lazy_key_burst_lmt;

    std::vector<std::function<std::string()>> steps;
    steps.push_back([&]() { return ""; }); // for align output
    steps.push_back([&]() {
      Utils::drop_file_cache(input_file);
      sorter = std::make_unique<whippet_sort::ParquetSorterTrie>(input_file,
                                                                 col_idx);
      sorter->set_trie_config(config);
      auto idx_array = sorter->sort_by_column();
      return "read+sort";
    });
    steps.push_back([&]() {
      sorter->generate_result();
#ifndef NDEBUG
      sorter->check_correctness();
#endif
      return "generate result";
    });
    auto [median, average] =
        Utils::benchmark("Trie", num_runs, std::move(steps));

    std::cout << "# Whippet sorting (Trie) - Median: " << median
              << "ms, Average: " << average << "ms" << std::endl;
  }

  if (FLAGS_trie_v2 || run_all) {
    std::unique_ptr<whippet_sort::ParquetSorterTrieV2> sorter;
    trie_v2::TrieConfig config;
    config.lazy_dep_lmt = FLAGS_trie_lazy_dep_lmt;
    config.lazy_key_burst_lmt = FLAGS_trie_lazy_key_burst_lmt;

    std::vector<std::function<std::string()>> steps;
    steps.push_back([&]() { return ""; }); // for align output
    steps.push_back([&]() {
      Utils::drop_file_cache(input_file);
      sorter = std::make_unique<whippet_sort::ParquetSorterTrieV2>(input_file,
                                                                   col_idx);
      sorter->set_trie_config(config);
      auto idx_array = sorter->sort_by_column();
      return "read+sort";
    });
    steps.push_back([&]() {
      sorter->generate_result();
#ifndef NDEBUG
      sorter->check_correctness();
#endif
      return "generate result";
    });
    auto [median, average] =
        Utils::benchmark("TrieV2", num_runs, std::move(steps));

    std::cout << "# Whippet sorting (TrieV2) - Median: " << median
              << "ms, Average: " << average << "ms" << std::endl;
  }
  // Check correctness
  // bool count_correct =
  //     check_whippet_sort_correctness("out_whippet_count.parquet", 0);
  // std::cout << "Count Base Whippet sort correctness: "
  //           << (count_correct ? "Correct" : "Incorrect") << std::endl;

  gflags::ShutDownCommandLineFlags();
  return 0;
}