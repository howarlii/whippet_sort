#pragma once

#include <arrow/status.h>
#include <cmath>
#include <deque>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/array/array_base.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/exec.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <glog/logging.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/encoding.h>
#include <parquet/file_reader.h>

namespace whippet_sort {
using std::deque;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

typedef uint32_t IndexType;

class ParquetSorterIf {
public:
  virtual ~ParquetSorterIf() = default;

  // Disable copy & move constructors for now to avoid unexpected behaviour.
  // Adjust later if needed.
  ParquetSorterIf(const ParquetSorterIf &) = delete;
  ParquetSorterIf &operator=(const ParquetSorterIf &) = delete;

  // Sort the column with the given index and return the sorted index list.
  virtual arrow::Result<std::shared_ptr<arrow::Array>> sort_by_column() = 0;

  auto &get_sort_index() const { return sort_index_; }

  virtual arrow::Status reorder_result() {
    throw std::runtime_error("Not implemented yet.");
    return arrow::Status::OK();
  }

  // Write the sorted table to the output file using index list
  virtual arrow::Status write(const std::string &output_file) {
    throw std::runtime_error("Not implemented yet.");
    return arrow::Status::OK();
  }

protected:
  // Only internal use. For construction, use create() instead.
  ParquetSorterIf(string input_file, uint32_t col_idx)
      : input_file_(std::move(input_file)), col_idx_(col_idx) {}

  static string ParquetPageTypeToString(parquet::PageType::type type) {
    switch (type) {
    case parquet::PageType::DATA_PAGE:
      return "DATA_PAGE";
    case parquet::PageType::DATA_PAGE_V2:
      return "DATA_PAGE_V2";
    case parquet::PageType::DICTIONARY_PAGE:
      return "DICTIONARY_PAGE";
    case parquet::PageType::INDEX_PAGE:
      return "INDEX_PAGE";
    case parquet::PageType::UNDEFINED:
      return "UNDEFINED";
    default:
      return "UNSUPPORTED PAGE TYPE";
    }
  }

  string input_file_;
  uint32_t col_idx_;
  std::shared_ptr<arrow::Array> sort_index_;
};

class ParquetSorterArrow : public ParquetSorterIf {
public:
  ParquetSorterArrow(string input_file, uint32_t col_idx)
      : ParquetSorterIf(std::move(input_file), col_idx) {
    if (open_file() != arrow::Status::OK()) {
      throw std::runtime_error("Failed to open input parquet file");
    }
  }

  virtual ~ParquetSorterArrow() = default;

  void print_column(int num_rows = 3) {
    LOG(INFO) << "number of num_chunks: " << column_->num_chunks();
    for (int i = 0; i < num_rows && i < column_->num_chunks(); ++i) {
      auto chunk = column_->chunk(i);
      LOG(INFO) << "Chunk " << i << " len: " << chunk->length()
                << ",  data: " << chunk->ToString() << std::endl;
    }
  }

  // Sort the column with the given index and return the sorted index list.
  arrow::Result<std::shared_ptr<arrow::Array>> sort_by_column() override;

  arrow::Status reorder_result() override {
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader_->ReadTable(&table));
    arrow::compute::TakeOptions take_options;
    ARROW_ASSIGN_OR_RAISE(
        auto ret,
        arrow::compute::Take(table, sort_index_, take_options, &exec_ctx_));
    sorted_table_ = ret.table();
    return arrow::Status::OK();
  }

  // arrow::Status write(const std::string &output_file) override;

private:
  arrow::Status open_file() {
    ARROW_ASSIGN_OR_RAISE(auto infile,
                          arrow::io::ReadableFile::Open(input_file_));

    // Create a ParquetFileReader
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(
        infile, arrow::default_memory_pool(), &reader_));

    // Read the entire file as a Table
    // ARROW_RETURN_NOT_OK(reader->ReadTable(&table_));

    ARROW_RETURN_NOT_OK(reader_->ReadColumn(col_idx_, &column_));
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::ChunkedArray> column_;
  std::unique_ptr<parquet::arrow::FileReader> reader_;
  std::shared_ptr<arrow::Table> sorted_table_;

  arrow::compute::ExecContext exec_ctx_;
};

} // namespace whippet_sort