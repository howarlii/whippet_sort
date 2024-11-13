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
              std::string(PROJECT_SOURCE_DIR) +
                  "/data/input-20-150-sed0.parquet",
              "Input file path");
DEFINE_int32(sort_col_idx, 2, "Column index to sort by");
DEFINE_double(std_dev_lmt, 0.15, "Standard deviation limit");
DEFINE_int32(num_runs, 5, "number of runs");
DEFINE_int32(warmup, 1, "number of warmup runs");
DEFINE_bool(debug, false, "Debug mode");

DEFINE_bool(hack_arrow, false, "Run high-level Arrow sorting benchmark");
DEFINE_bool(arrow, false, "Run low-level Arrow sorting benchmark");
DEFINE_bool(trie, false, "Run trie-based sorting benchmark");
DEFINE_bool(trie_v2, false, "Run trie-based sorting benchmark v2");
DEFINE_bool(trie_v2_bfs, false, "Run trie-based sorting benchmark v2 bfs");

DEFINE_int32(trie_lazy_dep_lmt, 4, "Trie lazy depth limit");
DEFINE_int32(trie_lazy_key_burst_lmt, 2048, "Trie lazy key burst limit");

int main(int argc, char *argv[]) {
  // Parse command line flags
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  nice(-20);
  const int num_runs = FLAGS_debug ? 1 : FLAGS_num_runs;

  const uint32_t col_idx = FLAGS_sort_col_idx;

  // Use the input_file flag
  std::string input_file = FLAGS_input_file;

  std::cout << "# input_file: " << input_file << ", col_idx: " << col_idx
            << std::endl;
  std::cout << "# trie_lazy_dep_lmt: " << FLAGS_trie_lazy_dep_lmt
            << ", trie_lazy_key_burst_lmt: " << FLAGS_trie_lazy_key_burst_lmt
            << std::endl;

  // Check if any flags were set, if not, run all benchmarks
  bool run_all = !FLAGS_hack_arrow && !FLAGS_arrow && !FLAGS_trie &&
                 !FLAGS_trie_v2 && !FLAGS_trie_v2_bfs;

  if (FLAGS_hack_arrow || run_all) {
    // Benchmark Arrow sorting
    std::vector<std::pair<std::string, Utils::BenchmarkStep>> steps;
    std::unique_ptr<whippet_sort::ParquetSorterHacked> sorter;
    steps.emplace_back("read", [&]() {
      // Utils::drop_file_cache(input_file);
      sorter = std::make_unique<whippet_sort::ParquetSorterHacked>(input_file,
                                                                   col_idx);
      sorter->read_all();
      return 0.0;
    });
    steps.emplace_back("sort", [&]() {
      // sorter.print_column();
      auto idx_array = sorter->sort_by_column();
      return 0.0;
    });
    // steps.emplace_back("generate result", [&]() {
    //   sorter->generate_result();
    //   return 0.0;
    // });
    auto [arrow_median_ms, arrow_average_ms, std_dev] =
        Utils::benchmark("hack-Arrow", num_runs, std::move(steps),
                         FLAGS_std_dev_lmt, FLAGS_warmup);

    std::cout << "# hack-Arrow sorting - Median: " << arrow_median_ms
              << "ms, Average: " << arrow_average_ms
              << "ms, std_dev: " << std_dev << std::endl;
  }

  if (FLAGS_arrow || run_all) {
    // Benchmark Arrow sorting
    std::vector<std::pair<std::string, Utils::BenchmarkStep>> steps;
    std::unique_ptr<whippet_sort::ParquetSorterHackedBinaryBuilder> sorter;
    steps.emplace_back("read", [&]() {
      // Utils::drop_file_cache(input_file);
      sorter = std::make_unique<whippet_sort::ParquetSorterHackedBinaryBuilder>(
          input_file, col_idx);
      sorter->read_all();
      return 0.0;
    });
    steps.emplace_back("sort", [&]() {
      // sorter.print_column();
      auto idx_array = sorter->sort_by_column();
      return 0.0;
    });
    // steps.emplace_back("generate result", [&]() {
    //   sorter->generate_result();
    //   return 0.0;
    // });
    auto [arrow_median_ms, arrow_average_ms, std_dev] =
        Utils::benchmark("hack-Arrow-BinaryArray", num_runs, std::move(steps),
                         FLAGS_std_dev_lmt, FLAGS_warmup);

    std::cout << "# hack-Arrow-BinaryArray sorting - Median: "
              << arrow_median_ms << "ms, Average: " << arrow_average_ms
              << "ms, std_dev: " << std_dev << std::endl;
  }

  if (false || run_all) {
    std::vector<std::pair<std::string, Utils::BenchmarkStep>> steps;
    std::unique_ptr<whippet_sort::ParquetSorterArrow> sorter;
    steps.emplace_back("read", [&]() {
      // Utils::drop_file_cache(input_file);
      sorter =
          std::make_unique<whippet_sort::ParquetSorterArrow>(input_file, 0);
      sorter->read_all();
      return 0.0;
    });
    steps.emplace_back("sort", [&]() {
      auto idx_array = sorter->sort_by_column();
      return 0.0;
    });
    // steps.emplace_back("generate result", [&]() {
    //   sorter->generate_result();
    //   return 0.0;
    // });
    auto [arrow_median_ms, arrow_average_ms, std_dev] = Utils::benchmark(
        "Arrow", num_runs, std::move(steps), FLAGS_std_dev_lmt, FLAGS_warmup);

    std::cout << "# Whippet sorting (Arrow) - Median: " << arrow_median_ms
              << "ms, Average: " << arrow_average_ms
              << "ms, std_dev: " << std_dev << std::endl;
  }

  if (FLAGS_trie || run_all) {
    std::unique_ptr<whippet_sort::ParquetSorterTrie> sorter;
    trie::TrieConfig config;
    config.lazy_dep_lmt = FLAGS_trie_lazy_dep_lmt;
    config.lazy_key_burst_lmt = FLAGS_trie_lazy_key_burst_lmt;
    config.index_only = !FLAGS_debug;
    double insert_time_ms = 0;

    std::vector<std::pair<std::string, Utils::BenchmarkStep>> steps;
    steps.emplace_back("read", [&]() {
      // Utils::drop_file_cache(input_file);

      struct timespec begin, end;
      clock_gettime(CLOCK_REALTIME, &begin);
      sorter = std::make_unique<whippet_sort::ParquetSorterTrie>(input_file,
                                                                 col_idx);
      sorter->set_trie_config(config);
      auto idx_array = sorter->sort_by_column();
      insert_time_ms = sorter->get_trie_builder()->get_insert_time_ms();
      clock_gettime(CLOCK_REALTIME, &end);
      return (end.tv_sec - begin.tv_sec) * 1e3 +
             (end.tv_nsec - begin.tv_nsec) / 1e6 - insert_time_ms;
    });
    steps.emplace_back("build", [&]() { return insert_time_ms; });

    steps.emplace_back("pre-sort", [&]() {
      sorter->pre_sort();
      return 0.0;
    });
    steps.emplace_back("print-trie", [&]() {
      sorter->print_trie();
      return 0.0;
    });
    if (FLAGS_debug) {
      steps.emplace_back("generate result", [&]() {
        sorter->generate_result();
        return 0.0;
      });
    }
    auto [median, average, std_dev] = Utils::benchmark(
        "Trie", num_runs, std::move(steps), FLAGS_std_dev_lmt, FLAGS_warmup);

    std::cout << "# Whippet sorting (Trie) - Median: " << median
              << "ms, Average: " << average << "ms,  std_dev: " << std_dev
              << std::endl;
    if (FLAGS_debug) {
      sorter->check_correctness();
    }
  }

  if (FLAGS_trie_v2 || run_all) {
    std::unique_ptr<whippet_sort::ParquetSorterTrieV2> sorter;
    trie_v2::TrieConfig config;
    // config.lazy_dep_lmt = FLAGS_trie_lazy_dep_lmt;
    config.lazy_key_burst_lmt = FLAGS_trie_lazy_key_burst_lmt;
    config.index_only = !FLAGS_debug;
    double insert_time_ms = 0;

    std::vector<std::pair<std::string, Utils::BenchmarkStep>> steps;
    steps.emplace_back("read", [&]() {
      // Utils::drop_file_cache(input_file);

      struct timespec begin, end;
      clock_gettime(CLOCK_REALTIME, &begin);
      sorter = std::make_unique<whippet_sort::ParquetSorterTrieV2>(input_file,
                                                                   col_idx);
      sorter->set_trie_builder(std::make_unique<trie_v2::TrieBuilder>(config));
      auto idx_array = sorter->sort_by_column();
      insert_time_ms = sorter->get_trie_builder()->get_insert_time_ms();
      clock_gettime(CLOCK_REALTIME, &end);
      return (end.tv_sec - begin.tv_sec) * 1e3 +
             (end.tv_nsec - begin.tv_nsec) / 1e6 - insert_time_ms;
    });
    steps.emplace_back("build", [&]() { return insert_time_ms; });

    steps.emplace_back("pre-sort", [&]() {
      sorter->pre_sort();
      if (FLAGS_debug) {
        sorter->statistics();
      }
      return 0.0;
    });
    steps.emplace_back("print-trie", [&]() {
      sorter->print_trie();
      return 0.0;
    });
    if (FLAGS_debug) {
      steps.emplace_back("generate result", [&]() {
        sorter->generate_result();
        return 0.0;
      });
    }
    auto [median, average, std_dev] = Utils::benchmark(
        "TrieV2", num_runs, std::move(steps), FLAGS_std_dev_lmt, FLAGS_warmup);

    std::cout << "# Whippet sorting (TrieV2) - Median: " << median
              << "ms, Average: " << average << "ms,  std_dev: " << std_dev
              << std::endl;
    if (FLAGS_debug) {
      sorter->check_correctness();
    }
  }

  if (FLAGS_trie_v2_bfs || run_all) {
    std::unique_ptr<whippet_sort::ParquetSorterTrieV2> sorter;
    trie_v2::TrieConfig config;
    // config.lazy_dep_lmt = FLAGS_trie_lazy_dep_lmt;
    config.lazy_key_burst_lmt = FLAGS_trie_lazy_key_burst_lmt;
    config.index_only = !FLAGS_debug;
    double insert_time_ms = 0;

    std::vector<std::pair<std::string, Utils::BenchmarkStep>> steps;
    steps.emplace_back("read", [&]() {
      // Utils::drop_file_cache(input_file);

      struct timespec begin, end;
      clock_gettime(CLOCK_REALTIME, &begin);
      sorter = std::make_unique<whippet_sort::ParquetSorterTrieV2>(input_file,
                                                                   col_idx);
      sorter->set_trie_builder(
          std::make_unique<trie_v2::TrieBuilderBfs>(config));
      auto idx_array = sorter->sort_by_column();
      insert_time_ms = sorter->get_trie_builder()->get_insert_time_ms();
      clock_gettime(CLOCK_REALTIME, &end);
      return (end.tv_sec - begin.tv_sec) * 1e3 +
             (end.tv_nsec - begin.tv_nsec) / 1e6 - insert_time_ms;
    });
    steps.emplace_back("build", [&]() { return insert_time_ms; });

    steps.emplace_back("pre-sort", [&]() {
      sorter->pre_sort();
      if (FLAGS_debug) {
        sorter->statistics();
      }
      return 0.0;
    });
    steps.emplace_back("print-trie", [&]() {
      sorter->print_trie();
      return 0.0;
    });
    if (FLAGS_debug) {
      steps.emplace_back("generate result", [&]() {
        sorter->generate_result();
        return 0.0;
      });
    }
    auto [median, average, std_dev] =
        Utils::benchmark("TrieV2Bfs", num_runs, std::move(steps),
                         FLAGS_std_dev_lmt, FLAGS_warmup);

    std::cout << "# Whippet sorting (TrieV2Bfs) - Median: " << median
              << "ms, Average: " << average << "ms,  std_dev: " << std_dev
              << std::endl;
    if (FLAGS_debug) {
      sorter->check_correctness();
    }
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}