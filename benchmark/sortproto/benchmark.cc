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
#include <iostream>
#include <string>
#include <vector>

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/encoding.h"
#include "parquet/file_reader.h"
#include "parquet/types.h"
#include "parquet_sorter.h"

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

  check_column_type(table, sort_column_index);
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

  // Write to Parquet Table
  // ARROW_ASSIGN_OR_RAISE(auto outfile,
  //                       arrow::io::FileOutputStream::Open(output_file));
  // PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
  //     *sorted_table, arrow::default_memory_pool(), outfile));
  // // Close the writer
  // PARQUET_THROW_NOT_OK(outfile->Close());
  return arrow::Status::OK();
}

void drop_file_cache(const std::string& file_path) {
  std::string command =
      "dd of=" + file_path + " oflag=nocache conv=notrunc,fdatasync count=0";

  int result = system(command.c_str());

  if (result != 0) {
    std::cerr << "Failed to drop file cache. Error code: " << result
              << std::endl;
  }
}

void whippet_sorting(const std::string& input_file) {
  using namespace whippet_sort;
  auto sorter = ParquetSorter::create(input_file, "output_1.parquet",
                                      SortStrategy::SortType::COUNT_BASE);
  auto index_list = sorter->sort_column(0);
  std::cout << "Num of rows: " << sorter->get_metadata()->num_rows()
            << std::endl
            << "Computed Row Index: " << index_list.size() << std::endl;
}

int main(const int argc, const char* argv[]) {
  nice(-20);
  auto input_file = std::string(argv[1]);
  drop_file_cache(input_file);
  auto start = std::chrono::high_resolution_clock::now();
  PARQUET_THROW_NOT_OK(arrow_sorting(input_file, "out_arrow.parquet"));
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Arrow sorting took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << " milliseconds." << std::endl;
  drop_file_cache(input_file);
  start = std::chrono::high_resolution_clock::now();
  whippet_sorting(input_file);
  end = std::chrono::high_resolution_clock::now();
  std::cout << "Whippet sorting took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << " milliseconds." << std::endl;

  // Clear the file cache of this file
}