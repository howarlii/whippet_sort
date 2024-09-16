#pragma once

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/builder_base.h>
#include <arrow/chunked_array.h>
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

    trie::TrieBuilder trie_builder;
    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      auto row_group = file_reader_->RowGroup(i);
      auto pager = row_group->GetColumnPageReader(col_idx_);

      auto col_sorter = std::make_unique<hack_parquet::ColumnTrieSorter<DType>>(
          column_descr, std::move(pager), nullptr);
      col_sorter->SetTrieBuilder(std::move(trie_builder));

      col_sorter->ReadAll(metadata_->RowGroup(i)->num_rows());

      trie_builder = std::move(col_sorter->GetTrieBuilder());

      CHECK(col_sorter->GetChunks().empty()) << "???";
    }
    trie_ = trie_builder.build();

    return sort_index_;
  }

  void generate_result() override {
    trie::TriePrinter printer(std::move(trie_));
    arrow::Int32Builder idx_builder;

    if (auto ret = idx_builder.Reserve(printer.valueNum()); !ret.ok()) {
      LOG(ERROR) << ret.message();
    }

    ::arrow::StringBuilder str_builder;
    if (!str_builder.Reserve(printer.valueNum()).ok()) {
      LOG(ERROR) << "Failed to reserve space for string builder.";
    }
    std::string last_str;
    while (printer.hasNext()) {
      size_t prefix_len;
      std::string key;
      int values;
      bool ret = printer.next(&prefix_len, &key, &values);
      if (!ret)
        break;

      if (auto ret = idx_builder.Append(values); !ret.ok()) {
        LOG(ERROR) << ret.message();
      }
      last_str = last_str.substr(0, prefix_len) + key;
      if (auto ret = str_builder.Append(last_str); !ret.ok()) {
        LOG(ERROR) << ret.message();
      }
      // res_a.emplace_back(std::move(key));
      // res_prefix_lens.push_back(prefix_len);
      // std::cout << prefix_len << " " << key << " " << values << std::endl;
    }
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

  std::unique_ptr<trie::Trie<int>> trie_;
};

class ParquetSorterTrieArrow : public ParquetSorterTrie {
public:
  // using DType = parquet::ByteArray;
  using DType = parquet::ByteArrayType;

  ParquetSorterTrieArrow(string input_file, uint32_t col_idx)
      : ParquetSorterTrie(std::move(input_file), col_idx) {}

  void read_all() {
    // Sort the column with the given index and return the sorted index list.
    if (col_idx_ >= metadata_->num_columns()) {
      LOG(ERROR) << "Column index out of range.";
    }

    auto column_descr = metadata_->schema()->Column(col_idx_);
    if (column_descr->physical_type() != DType::type_num) {
      LOG(ERROR) << "Column is not a BYTE_ARRAY column.";
    }

    std::vector<std::shared_ptr<::arrow::Array>> all_chunks;

    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      auto row_group = file_reader_->RowGroup(i);
      auto pager = row_group->GetColumnPageReader(col_idx_);

      auto col_sorter = std::make_unique<hack_parquet::ColumnTrieSorter<DType>>(
          column_descr, std::move(pager), nullptr);

      col_sorter->ReadAll(metadata_->RowGroup(i)->num_rows());

      auto trie_builder = std::move(col_sorter->GetTrieBuilder());
      CHECK_EQ(trie_builder.valueNum(), 0) << "???";
      if (auto chunks = col_sorter->GetChunks(); !chunks.empty()) {
        all_chunks.insert(all_chunks.end(), chunks.begin(), chunks.end());
      }
    }

    origin_column_ =
        std::make_shared<::arrow::ChunkedArray>(std::move(all_chunks));
  }

  std::shared_ptr<arrow::Array> sort_by_column() override {
    arrow::compute::ExecContext exec_ctx_;
    arrow::compute::SortOptions sort_options;
    auto ret =
        arrow::compute::SortIndices(origin_column_, sort_options, &exec_ctx_);
    if (ret.ok()) {
      sort_index_ = ret.ValueOrDie();
    } else {
      LOG(ERROR) << ret.status().message();
    }

    return sort_index_;
  }

  void generate_result() override {
    if (sort_index_) {
      arrow::compute::ExecContext exec_ctx;
      arrow::compute::TakeOptions take_options;
      auto ret = arrow::compute::Take(origin_column_, sort_index_, take_options,
                                      &exec_ctx);
      sorted_column_ = ret.ValueOrDie().chunked_array();
      return;
    }
    // trival all the nodes
    CHECK(false) << "Sort index is not available.";
  }

protected:
  std::shared_ptr<arrow::ChunkedArray> origin_column_;
};

} // namespace whippet_sort