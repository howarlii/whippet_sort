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
#include "parquet_sorter_dict_tree.h"

void drop_file_cache(const std::string &file_path) {
  std::string command =
      "dd of=" + file_path +
      " oflag=nocache conv=notrunc,fdatasync status=none count=0";
  auto drop_cache = system(command.c_str());
  if (drop_cache != 0) {
    std::cerr << "Failed to drop file cache. Error code: " << drop_cache
              << std::endl;
  }
}

void check_column_type(const std::shared_ptr<arrow::Table> &table,
                       int column_index) {
  auto column = table->column(column_index);
  auto type = column->type();

  std::cout << "Column " << column_index << " type: " << type->ToString()
            << std::endl;

  if (type->id() == arrow::Type::DICTIONARY) {
    auto dict_type = std::static_pointer_cast<arrow::DictionaryType>(type);
    std::cout << "  This is a dictionary-encoded column." << std::endl;
    std::cout << "  Index type: " << dict_type->index_type()->ToString()
              << std::endl;
    std::cout << "  Value type: " << dict_type->value_type()->ToString()
              << std::endl;
  } else if (type->id() == arrow::Type::STRING) {
    auto string_type = std::static_pointer_cast<arrow::StringType>(type);
    std::cout << "  This is a string column." << std::endl;
  }
}

std::pair<double, double> benchmark(std::function<void()> &&func,
                                    int num_runs) {
  std::vector<double> durations;
  durations.reserve(num_runs);

  for (int i = 0; i < num_runs; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();

    double duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    durations.push_back(duration);
  }

  // Calculate average
  double average =
      std::accumulate(durations.begin(), durations.end(), 0.0) / num_runs;

  // Calculate median
  std::sort(durations.begin(), durations.end());
  double median = durations[num_runs / 2];

  return {median, average};
}

bool check_whippet_sort_correctness(const std::string &parquet_file,
                                    int sorted_column_index) {
  std::shared_ptr<arrow::io::RandomAccessFile> input_file;
  auto state = arrow::io::ReadableFile::Open(parquet_file);
  if (!state.ok()) {
    std::cerr << "Failed to open input file." << std::endl;
    throw std::runtime_error("Failed to open input parquet file");
  }

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::Open(input_file);

  std::shared_ptr<parquet::FileMetaData> file_metadata =
      parquet_reader->metadata();

  if (sorted_column_index >= file_metadata->num_columns()) {
    std::cerr << "Invalid column index." << std::endl;
    return false;
  }

  // Read the column data
  std::shared_ptr<parquet::ColumnReader> column_reader =
      parquet_reader->RowGroup(0)->Column(sorted_column_index);

  parquet::Int64Reader *int64_reader =
      static_cast<parquet::Int64Reader *>(column_reader.get());

  int64_t values[1000];
  int64_t values_read;
  int64_t previous_value = std::numeric_limits<int64_t>::min();

  while (int64_reader->HasNext()) {
    int64_reader->ReadBatch(1000, nullptr, nullptr, values, &values_read);

    for (int64_t i = 0; i < values_read; ++i) {
      if (values[i] < previous_value) {
        std::cerr << "Column is not sorted at index " << i << std::endl;
        return false;
      }
      previous_value = values[i];
    }
  }

  return true;
}

int main(const int argc, const char *argv[]) {
  nice(-20);
  const int num_runs = 1;
  const uint32_t col_idx = 0;

  std::string input_file = "input.parquet";
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
  auto [arrow_median, arrow_average] = benchmark(
      [&]() {
        drop_file_cache(input_file);
        whippet_sort::ParquetSorterArrow sorter(input_file, col_idx);
        sorter.print_column();
        //  PARQUET_THROW_NOT_OK(sorter.sort_by_column(0));
      },
      num_runs);

  std::cout << "Arrow sorting - Median: " << arrow_median
            << "ms, Average: " << arrow_average << "ms" << std::endl;

  auto [whippet_dict_tree_mid, whippet_dict_tree_avg] = benchmark(
      [&]() {
        drop_file_cache(input_file);
        whippet_sort::ParquetSorterDictTree sorter(input_file, col_idx);
        auto ret = sorter.sort_by_column_arrow();
        ret = sorter.sort_by_column();
      },
      num_runs);

  std::cout << "Whippet sorting (Dictionary Tree) - Median: "
            << whippet_dict_tree_mid << "ms, Average: " << whippet_dict_tree_avg
            << "ms" << std::endl;

  // Check correctness
  // bool count_correct =
  //     check_whippet_sort_correctness("out_whippet_count.parquet", 0);
  // std::cout << "Count Base Whippet sort correctness: "
  //           << (count_correct ? "Correct" : "Incorrect") << std::endl;

  return 0;
}