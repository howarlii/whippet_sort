#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
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
  const int num_runs = 1;
  const uint32_t col_idx = 0;

  std::string input_file = "data/input.parquet";
  if (argc > 1) {
    input_file = std::string(argv[1]);
  }

  // Report the number of row groups:
  // auto sorter = whippet_sort::ParquetSorter::create(
  //     input_file, "output_file",
  //     whippet_sort::SortStrategy::SortType::COUNT_BASE);
  // std::cout << "Number of RowGroups: "
  //           << sorter->file_reader->metadata()->num_row_groups() <<
  //           std::endl;

  // Benchmark Arrow sorting
  auto [arrow_median, arrow_average] = Utils::benchmark(
      [&]() {
        Utils::drop_file_cache(input_file);
        whippet_sort::ParquetSorterArrow sorter(input_file, col_idx);
        sorter.print_column();
        //  PARQUET_THROW_NOT_OK(sorter.sort_by_column(0));
      },
      num_runs);

  std::cout << "Arrow sorting - Median: " << arrow_median
            << "ms, Average: " << arrow_average << "ms" << std::endl;

  auto [whippet_trie__mid, whippet_trie__avg] = Utils::benchmark(
      [&]() {
        Utils::drop_file_cache(input_file);
        whippet_sort::ParquetSorterArrow2 sorter2(input_file, col_idx);
        auto ret = sorter2.sort_by_column();
        whippet_sort::ParquetSorterTrie sorter(input_file, col_idx);
        ret = sorter.sort_by_column();
      },
      num_runs);

  std::cout << "Whippet sorting (Dictionary Tree) - Median: "
            << whippet_trie__mid << "ms, Average: " << whippet_trie__avg << "ms"
            << std::endl;

  // Check correctness
  // bool count_correct =
  //     check_whippet_sort_correctness("out_whippet_count.parquet", 0);
  // std::cout << "Count Base Whippet sort correctness: "
  //           << (count_correct ? "Correct" : "Incorrect") << std::endl;

  return 0;
}