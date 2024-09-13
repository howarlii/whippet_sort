#pragma once

#include <cassert>
#include <cmath>
#include <deque>
#include <functional>
#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "parquet_sorter.h"
#include "trie_sort/hack_column_reader.h"
#include "trie_sort/trie_sort.h"

#include <arrow/io/api.h>
#include <arrow/result.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/encoding.h>
#include <parquet/file_reader.h>

namespace whippet_sort {

typedef uint32_t IndexType;

class ParquetSorterTrie : public ParquetSorterIf {
public:
  // using DType = parquet::ByteArray;
  using DType = parquet::ByteArrayType;

  ParquetSorterTrie(string input_file, uint32_t col_idx)
      : ParquetSorterIf(std::move(input_file), col_idx) {
    open_file();
  }

  // Sort the column with the given index and return the sorted index list.
  std::shared_ptr<arrow::Array> sort_by_column() override {
    if (col_idx_ >= metadata_->num_columns()) {
      LOG(ERROR) << "Column index out of range.";
    }

    auto column_descr = metadata_->schema()->Column(col_idx_);
    if (column_descr->physical_type() != DType::type_num) {
      LOG(ERROR) << "Column is not a BYTE_ARRAY column.";
    }

    std::vector<std::shared_ptr<::arrow::Array>> all_chunks;
    std::vector<std::unique_ptr<trie::Trie<int>>> tries;
    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      auto row_group = file_reader_->RowGroup(i);
      auto pager = row_group->GetColumnPageReader(col_idx_);

      // auto col_reader =
      // std::make_unique<hack_parquet::ColumnTrieSorter<DType>>(
      //     column_descr, std::move(pager), nullptr);

      // auto tot_num_values = row_group->metadata()->num_rows();
      // std::vector<DType::c_type> values_view(tot_num_values);
      // std::vector<std::string> values(tot_num_values);
      // int64_t values_read = 0;
      // while (col_reader->HasNext()) {
      //   auto read_num = col_reader->ReadValues(tot_num_values - values_read,
      //                                          &values_view[values_read]);
      //   for (int i = values_read; i < values_read + read_num; ++i) {
      //     values[i] =
      //         std::string(reinterpret_cast<const char *>(values_view[i].ptr),
      //                     values_view[i].len);
      //   }
      //   values_read += read_num;
      //   CHECK_LE(values_read, tot_num_values);
      //   LOG(INFO) << "Read " << read_num << " values.   " << values_read;
      // }

      auto col_sorter = std::make_unique<hack_parquet::ColumnTrieSorter<DType>>(
          column_descr, std::move(pager), nullptr);

      col_sorter->ReadAll();
      auto chunks = col_sorter->GetChunks();
      if (chunks.empty()) {
        tries.emplace_back(col_sorter->GetTrie());

      } else {
        all_chunks.insert(all_chunks.end(), chunks.begin(), chunks.end());
      }
    }

    CHECK(tries.empty() ^ all_chunks.empty());

    if (all_chunks.size()) {
      auto chunked_array =
          std::make_shared<::arrow::ChunkedArray>(std::move(all_chunks));
      arrow::compute::ExecContext exec_ctx_;
      arrow::compute::SortOptions sort_options;
      auto ret =
          arrow::compute::SortIndices(chunked_array, sort_options, &exec_ctx_);
      if (ret.ok()) {
        sort_index_ = ret.ValueOrDie();
      } else {
        LOG(ERROR) << ret.status().message();
      }
    } else {
      CHECK(false) << " Not implemented yet.";
    }

    return sort_index_;
  }

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

protected:
  void open_file() {
    std::shared_ptr<arrow::io::RandomAccessFile> file;
    auto state = arrow::io::ReadableFile::Open(input_file_);
    if (!state.ok()) {
      LOG(INFO) << "Failed to open input file.";
      throw std::runtime_error("Failed to open input parquet file");
    }
    file = state.ValueOrDie();
    file_reader_ = parquet::ParquetFileReader::Open(file);
    metadata_ = file_reader_->metadata();
  }

  unique_ptr<parquet::ParquetFileReader> file_reader_;
  shared_ptr<parquet::FileMetaData> metadata_;

  std::shared_ptr<arrow::Table> sorted_table_;
};

class ParquetSorterArrow2 : public ParquetSorterTrie {
public:
  ParquetSorterArrow2(string input_file, uint32_t col_idx)
      : ParquetSorterTrie(std::move(input_file), col_idx) {}

  std::shared_ptr<arrow::Array> sort_by_column() override {
    if (col_idx_ >= metadata_->num_columns()) {
      LOG(ERROR) << "Column index out of range.";
    }

    auto column_descr = metadata_->schema()->Column(col_idx_);
    if (column_descr->physical_type() != DType::type_num) {
      LOG(ERROR) << "Column is not a BYTE_ARRAY column.";
    }

    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      auto row_group = file_reader_->RowGroup(i);
      auto string_reader = std::dynamic_pointer_cast<parquet::ByteArrayReader>(
          row_group->Column(col_idx_));

      auto tot_num_values = row_group->metadata()->num_rows();
      std::vector<DType::c_type> values_view(tot_num_values * 2);
      std::vector<std::string> values(tot_num_values * 2);

      int64_t values_read = 0;
      while (string_reader->HasNext()) {
        int64_t read_num = 0;
        string_reader->ReadBatch(tot_num_values - values_read, nullptr, nullptr,
                                 &values_view[values_read], &read_num);
        for (int i = values_read; i < values_read + read_num; ++i) {
          values[i] =
              std::string(reinterpret_cast<const char *>(values_view[i].ptr),
                          values_view[i].len);
        }
        values_read += read_num;
        CHECK_LE(values_read, tot_num_values);
        // LOG(INFO) << "Read " << read_num << " values.   " << values_read;
      }

      LOG(INFO) << "number of rows: " << values_read;
      for (int i = 0; i < 3 && i < values_read; ++i) {
        LOG(INFO) << fmt::format("Value {}: {}", i, values[i]);
      }
      CHECK_EQ(values_read, tot_num_values);

      std::size_t hash = std::hash<uint32_t>()(tot_num_values);
      for (int i = 0; i < tot_num_values; ++i) {
        hash ^= std::hash<std::string>()(values[i]) + 0x9e3779b9 + (hash << 6) +
                (hash >> 2);
      }
      LOG(INFO) << "==========> hash: " << hash;
    }
    return nullptr;
  }
};

} // namespace whippet_sort