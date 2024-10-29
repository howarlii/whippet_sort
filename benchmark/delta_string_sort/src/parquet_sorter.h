#pragma once

#include <arrow/array/builder_binary.h>
#include <arrow/status.h>
#include <cmath>
#include <deque>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/compute/api.h>
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

#include "utils.h"

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
  virtual std::shared_ptr<arrow::Array> sort_by_column() = 0;

  auto &get_sort_index() const { return sort_index_; }
  auto &get_sorted_column() const { return sorted_column_; }

  virtual void generate_result() {
    throw std::runtime_error("Not implemented yet.");
  }

  // Write the sorted table to the output file using index list
  virtual arrow::Status write(const std::string &output_file) {
    throw std::runtime_error("Not implemented yet.");
    return arrow::Status::OK();
  }

  // calc the hash of sorted_table_
  virtual size_t compute_hash() {
    CHECK(sorted_column_ != nullptr) << "No sorted column found.";
    std::size_t final_hash = 0;
    for (int i = 0; i < sorted_column_->num_chunks(); ++i) {
      std::shared_ptr<arrow::Array> chunk = sorted_column_->chunk(i);

      // Hash the current chunk (use compute API or your own logic)
      if (chunk->type_id() == arrow::Type::STRING) {
        auto array = std::dynamic_pointer_cast<arrow::StringArray>(chunk);
        for (int64_t j = 0; j < array->length(); ++j) {
          final_hash = Utils::hashCombine(final_hash, array->Value(j));
        }
      } else if (chunk->type_id() == arrow::Type::LARGE_STRING) {
        auto array = std::dynamic_pointer_cast<arrow::LargeStringArray>(chunk);
        for (int64_t j = 0; j < array->length(); ++j) {
          final_hash = Utils::hashCombine(final_hash, array->Value(j));
        }
      }
    }
    return final_hash;
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
  int64_t num_rows_;

  std::shared_ptr<arrow::Array> sort_index_;
  std::shared_ptr<arrow::ChunkedArray> sorted_column_;
};

class ParquetSorterArrow : public ParquetSorterIf {
public:
  ParquetSorterArrow(string input_file, uint32_t col_idx)
      : ParquetSorterIf(std::move(input_file), col_idx) {}

  virtual ~ParquetSorterArrow() = default;

  void read_all() {
    if (open_file() != arrow::Status::OK()) {
      throw std::runtime_error("Failed to open input parquet file");
    }
    num_rows_ = column_->length();
  }

  void print_column(int num_rows = 3) {
    LOG(INFO) << "number of num_chunks: " << column_->num_chunks();
    for (int i = 0; i < num_rows && i < column_->num_chunks(); ++i) {
      auto chunk = column_->chunk(i);
      LOG(INFO) << "Chunk " << i << " len: " << chunk->length()
                << ",  data: " << chunk->ToString() << std::endl;
    }
  }

  // Sort the column with the given index and return the sorted index list.
  std::shared_ptr<arrow::Array> sort_by_column() override;

  void generate_result() override {
    arrow::compute::TakeOptions take_options;

    auto ret =
        arrow::compute::Take(column_, sort_index_, take_options, &exec_ctx_);

    sorted_column_ = ret.ValueOrDie().chunked_array();
    DCHECK_EQ(sorted_column_->length(), num_rows_);
    // sorted_column_ = sorted_table_->column(col_idx_);
  }

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
    DLOG(INFO) << "column type: " << column_->type()->ToString();

    // Convert to LargeStringArray if needed
    if (column_->type()->id() == arrow::Type::STRING) {
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = arrow::large_utf8();
      auto result = arrow::compute::Cast(column_, cast_options, &exec_ctx_);
      if (!result.ok()) {
        return arrow::Status::Invalid(
            "Failed to cast StringArray to LargeStringArray");
      }
      column_ = result.ValueOrDie().chunked_array();
    }

    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::ChunkedArray> column_;
  std::unique_ptr<parquet::arrow::FileReader> reader_;
  std::shared_ptr<arrow::Table> sorted_table_;

  arrow::compute::ExecContext exec_ctx_;
};

// class ParquetSorterArrow : public ParquetSorterIf {
// public:
//   // using DType = parquet::ByteArray;
//   using DType = parquet::ByteArrayType;

//   ParquetSorterArrow(string input_file, uint32_t col_idx)
//       : ParquetSorterIf(std::move(input_file), col_idx) {
//     open_file();
//   }

//   void read_all() {
//     // Sort the column with the given index and return the sorted index list.
//     if (col_idx_ >= metadata_->num_columns()) {
//       LOG(ERROR) << "Column index out of range.";
//     }

//     auto column_descr = metadata_->schema()->Column(col_idx_);
//     if (column_descr->physical_type() != DType::type_num) {
//       LOG(ERROR) << "Column is not a BYTE_ARRAY column.";
//     }

//     auto array_builder = std::make_shared<::arrow::LargeStringBuilder>();

//     std::vector<parquet::ByteArray> values(1e5);
//     for (int i = 0; i < metadata_->num_row_groups(); ++i) {
//       auto row_group = file_reader_->RowGroup(i);
//       auto column_reader = row_group->Column(col_idx_);

//       auto byte_array_reader =
//           static_cast<parquet::ByteArrayReader *>(column_reader.get());
//       while (byte_array_reader->HasNext()) {
//         int64_t values_read;
//         // Read one value at a time. The number of rows read is returned.
//         // values_read contains the number of non-null rows
//         int64_t rows_read = byte_array_reader->ReadBatch(
//             values.size(), nullptr, nullptr, values.data(), &values_read);
//         DCHECK_EQ(rows_read, values_read);
//         for (int64_t i = 0; i < values_read; ++i) {
//           if (auto ret = array_builder->Append(values[i].ptr, values[i].len);
//               !ret.ok()) {
//             LOG(ERROR) << ret.message();
//           }
//         }
//       }
//     }
//     std::shared_ptr<::arrow::Array> array;
//     if (auto ret = array_builder->Finish(&array); !ret.ok()) {
//       LOG(ERROR) << ret.message();
//     }
//     origin_column_ = std::make_shared<::arrow::ChunkedArray>(array);
//   }

//   std::shared_ptr<arrow::Array> sort_by_column() override {
//     arrow::compute::ExecContext exec_ctx_;
//     arrow::compute::SortOptions sort_options;
//     auto ret =
//         arrow::compute::SortIndices(origin_column_, sort_options,
//         &exec_ctx_);
//     if (ret.ok()) {
//       sort_index_ = ret.ValueOrDie();
//     } else {
//       LOG(ERROR) << ret.status().message();
//     }

//     return sort_index_;
//   }

//   void generate_result() override {
//     if (sort_index_) {
//       arrow::compute::ExecContext exec_ctx;
//       arrow::compute::TakeOptions take_options;
//       auto ret = arrow::compute::Take(origin_column_, sort_index_,
//       take_options,
//                                       &exec_ctx);
//       sorted_column_ = ret.ValueOrDie().chunked_array();
//       return;
//     }
//     // trival all the nodes
//     CHECK(false) << "Sort index is not available.";
//   }

//   bool check_correctness() {
//     if (!sorted_column_ || sorted_column_->num_chunks() == 0) {
//       LOG(ERROR) << "Sorted column is empty or not initialized.";
//       return false;
//     }

//     std::string prev_str = "";
//     for (int chunk_i = 0; chunk_i < sorted_column_->num_chunks(); ++chunk_i)
//     {
//       auto str_array = std::static_pointer_cast<arrow::LargeStringArray>(
//           sorted_column_->chunk(chunk_i));
//       for (int64_t i = 0; i < str_array->length(); ++i) {
//         std::string curr_str = str_array->GetString(i);
//         if (curr_str < prev_str) {
//           LOG(ERROR) << "Sorting error at index " << i << ": " << curr_str
//                      << " < " << prev_str;
//           return false;
//         }
//         prev_str = curr_str;
//       }
//     }

//     return true;
//   }

// protected:
//   void open_file() {
//     std::shared_ptr<arrow::io::RandomAccessFile> file;
//     auto state = arrow::io::ReadableFile::Open(input_file_);
//     if (!state.ok()) {
//       LOG(INFO) << "Failed to open input file.";
//       throw std::runtime_error("Failed to open input parquet file");
//     }
//     file = state.ValueOrDie();
//     file_reader_ = parquet::ParquetFileReader::Open(file);
//     metadata_ = file_reader_->metadata();
//   }

//   unique_ptr<parquet::ParquetFileReader> file_reader_;
//   shared_ptr<parquet::FileMetaData> metadata_;

//   std::shared_ptr<arrow::ChunkedArray> origin_column_;
// };

} // namespace whippet_sort