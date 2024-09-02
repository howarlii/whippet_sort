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
using std::pair;

void drop_file_cache(const std::string& file_path) {
  std::string command =
      "dd of=" + file_path +
      " oflag=nocache conv=notrunc,fdatasync status=none count=0";
  auto drop_cache = system(command.c_str());
  if (drop_cache != 0) {
    std::cerr << "Failed to drop file cache. Error code: " << drop_cache
              << std::endl;
  }
}

void check_column_type(const std::shared_ptr<arrow::Table>& table,
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
  }
}

arrow::Status arrow_sorting(const std::string& input_file,
                            const std::string& output_file) {
  // Open the input file
  ARROW_ASSIGN_OR_RAISE(auto infile, arrow::io::ReadableFile::Open(input_file));

  // Create a ParquetFileReader
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

  // Read the entire file as a Table
  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  int sort_column_index = 0;

  // Get the column to sort
  std::shared_ptr<arrow::ChunkedArray> column =
      table->column(sort_column_index);
  // Sort the column
  arrow::compute::ExecContext ctx;
  arrow::compute::SortOptions sort_options;
  arrow::compute::TakeOptions take_options;
  ARROW_ASSIGN_OR_RAISE(auto sort_indices, arrow::compute::SortIndices(
                                               column, sort_options, &ctx));
  // ARROW_ASSIGN_OR_RAISE(auto result, arrow::compute::Take(table,
  // sort_indices,
  //                                                         take_options,
  //                                                         &ctx));

  // shared_ptr<arrow::Table> sorted_table = result.table();

  // ARROW_ASSIGN_OR_RAISE(auto outfile,
  //                       arrow::io::FileOutputStream::Open(output_file));
  // PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
  //     *sorted_table, arrow::default_memory_pool(), outfile));
  // // Close the writer
  // PARQUET_THROW_NOT_OK(outfile->Close());
  return arrow::Status::OK();
}

void whippet_sorting(const std::string& input_file,
                     const std::string& output_file,
                     whippet_sort::SortStrategy::SortType sort_type) {
  using namespace whippet_sort;
  auto sorter = ParquetSorter::create(input_file, output_file, sort_type);
  auto index_list = sorter->sort_column(0);
  // Prevent compiler to remove the code
  index_list[0] = index_list[10];  // TODO: DEBUG write function(ZIJIE)
  if (sort_type == SortStrategy::SortType::INDEX_BASE) {
    std::cout << "Index length: " << index_list.size() << std::endl;
  }
  // auto status = sorter->write(std::move(index_list));
  // if (!status.ok()) {
  //   std::cerr << "Failed to write sorted table to output file." << std::endl;
  //   throw std::runtime_error("Failed to write sorted table to output file.");
  // }
}

template <typename Func>
pair<double, double> benchmark(Func&& func, int num_runs) {
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

bool check_whippet_sort_correctness(const std::string& parquet_file,
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

  parquet::Int64Reader* int64_reader =
      static_cast<parquet::Int64Reader*>(column_reader.get());

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
int main(const int argc, const char* argv[]) {
  nice(-20);
  int num_runs = 20;
  auto input_file = std::string(argv[1]);
  // Report the number of row groups:
  auto sorter = whippet_sort::ParquetSorter::create(
      input_file, "output_file",
      whippet_sort::SortStrategy::SortType::COUNT_BASE);
  std::cout << "Number of RowGroups: "
            << sorter->file_reader->metadata()->num_row_groups() << std::endl;

  // Benchmark Arrow sorting
  auto [arrow_median, arrow_average] = benchmark(
      [&]() {
        drop_file_cache(input_file);
        PARQUET_THROW_NOT_OK(arrow_sorting(input_file, "out_arrow.parquet"));
      },
      num_runs);

  std::cout << "Arrow sorting - Median: " << arrow_median
            << "ms, Average: " << arrow_average << "ms" << std::endl;

  // Benchmark Whippet sorting (CountBaseSort)
  auto [whippet_count_median, whippet_count_average] = benchmark(
      [&]() {
        drop_file_cache(input_file);
        whippet_sorting(input_file, "out_whippet_count.parquet",
                        whippet_sort::SortStrategy::SortType::COUNT_BASE);
      },
      num_runs);

  std::cout << "Whippet sorting (CountBaseSort) - Median: "
            << whippet_count_median << "ms, Average: " << whippet_count_average
            << "ms" << std::endl;

  // Benchmark Whippet sorting (IndexBaseSort)
  auto [whippet_index_median, whippet_index_average] = benchmark(
      [&]() {
        drop_file_cache(input_file);
        whippet_sorting(input_file, "out_whippet_index.parquet",
                        whippet_sort::SortStrategy::SortType::INDEX_BASE);
      },
      num_runs);

  std::cout << "Whippet sorting (IndexBaseSort) - Median: "
            << whippet_index_median << "ms, Average: " << whippet_index_average
            << "ms" << std::endl;

  // Check correctness
  // bool count_correct =
  //     check_whippet_sort_correctness("out_whippet_count.parquet", 0);
  // std::cout << "Count Base Whippet sort correctness: "
  //           << (count_correct ? "Correct" : "Incorrect") << std::endl;
  // bool index_correct =
  //     check_whippet_sort_correctness("out_whippet_index.parquet", 0);
  // std::cout << "Index Base Whippet sort correctness: "
  //           << (index_correct ? "Correct" : "Incorrect") << std::endl;
  return 0;
}