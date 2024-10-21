#pragma once

#include <cassert>
#include <cmath>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "parquet_sorter.h"
#include "trie_sort/hack_column_reader.h"
#include "trie_sort/trie_sort_v2.h"

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/builder_base.h>
#include <arrow/chunked_array.h>
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

class ParquetSorterTrieV2 : public ParquetSorterIf {
public:
  // using DType = parquet::ByteArray;
  using DType = parquet::ByteArrayType;

  ParquetSorterTrieV2(string input_file, uint32_t col_idx)
      : ParquetSorterIf(std::move(input_file), col_idx) {
    open_file();
  }

  void set_trie_config(const trie_v2::TrieConfig &config) {
    trie_config_ = config;
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

    auto trie_builder = std::make_unique<trie_v2::TrieBuilder>(trie_config_);
    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      auto row_group = file_reader_->RowGroup(i);
      auto pager = row_group->GetColumnPageReader(col_idx_);

      auto col_sorter = std::make_unique<hack_parquet::ColumnTrieSorter<DType>>(
          column_descr, std::move(pager), nullptr);
      col_sorter->SetTrieBuilder(trie_builder.get());

      col_sorter->ReadAll(metadata_->RowGroup(i)->num_rows());

      CHECK(col_sorter->GetChunks().empty()) << "???";
    }
    trie_ = trie_builder->build();

    return sort_index_;
  }

  void pre_sort() {
    printer_ = std::make_unique<trie_v2::TriePrinter>(std::move(trie_));
    printer_->preSort();
  }

  void generate_result() override {
    arrow::Int32Builder idx_builder;
    if (auto ret = idx_builder.Reserve(printer_->valueNum()); !ret.ok()) {
      LOG(ERROR) << ret.message();
    }
    ::arrow::StringBuilder str_builder;
    if (!str_builder.Reserve(printer_->valueNum()).ok()) {
      LOG(ERROR) << "Failed to reserve space for string builder.";
    }

    std::string last_str;
    auto f = [&](size_t prefix_len, std::string key, int value) {
      if (auto ret = idx_builder.Append(value); !ret.ok()) {
        LOG(ERROR) << ret.message();
      }
      last_str = last_str.substr(0, prefix_len) + key;
      if (auto ret = str_builder.Append(last_str); !ret.ok()) {
        LOG(ERROR) << ret.message();
      }
    };

    printer_->registerFunc(f);
    printer_->print();

    if (auto ret = idx_builder.Finish(&sort_index_); !ret.ok()) {
      LOG(ERROR) << ret.message();
    }
    std::shared_ptr<arrow::Array> str_array;
    if (auto ret = str_builder.Finish(&str_array); !ret.ok()) {
      LOG(ERROR) << ret.message();
    }
    sorted_column_ =
        std::make_shared<::arrow::ChunkedArray>(std::move(str_array));
  }

  bool check_correctness() {
    if (!sorted_column_ || sorted_column_->num_chunks() == 0) {
      LOG(ERROR) << "Sorted column is empty or not initialized.";
      return false;
    }

    std::string prev_str = "";
    for (int chunk_i = 0; chunk_i < sorted_column_->num_chunks(); ++chunk_i) {
      auto str_array = std::static_pointer_cast<arrow::StringArray>(
          sorted_column_->chunk(chunk_i));
      for (int64_t i = 0; i < str_array->length(); ++i) {
        std::string curr_str = str_array->GetString(i);
        if (curr_str < prev_str) {
          LOG(ERROR) << "Sorting error at index " << i << ": " << curr_str
                     << " < " << prev_str;
          return false;
        }
        prev_str = curr_str;
      }
    }

    return true;
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

  trie_v2::TrieConfig trie_config_;
  std::unique_ptr<trie_v2::Trie> trie_;
  std::unique_ptr<trie_v2::TriePrinter> printer_;
};

} // namespace whippet_sort