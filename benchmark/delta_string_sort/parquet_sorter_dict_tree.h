#pragma once

#include <cmath>
#include <deque>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "hack_column_reader.h"
#include "parquet_sorter.h"

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

class ParquetSorterDictTree : public ParquetSorterIf {
  // using DType = parquet::ByteArray;
  using DType = parquet::ByteArrayType;

public:
  ParquetSorterDictTree(string input_file, uint32_t col_idx)
      : ParquetSorterIf(std::move(input_file), col_idx) {
    open_file();
  }

  // Sort the column with the given index and return the sorted index list.
  arrow::Result<std::shared_ptr<arrow::Array>> sort_by_column() override {
    if (col_idx_ >= metadata_->num_columns()) {
      LOG(ERROR) << "Column index out of range.";
      return arrow::Status::Invalid("Column index out of range.");
    }

    auto column_descr = metadata_->schema()->Column(col_idx_);
    if (column_descr->physical_type() != DType::type_num) {
      LOG(ERROR) << "Column is not a BYTE_ARRAY column.";
      return arrow::Status::Invalid("Column is not a BYTE_ARRAY column.");
    }

    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      auto row_group = file_reader_->RowGroup(i);
      auto pager = row_group->GetColumnPageReader(col_idx_);

      auto col_reader =
          std::make_unique<hack_parquet::TypedColumnReaderImpl<DType>>(
              column_descr, std::move(pager), nullptr);

      auto num_values = row_group->metadata()->num_rows();
      std::vector<DType::c_type> values(num_values);
      auto ret = col_reader->ReadValues(num_values, &values[0]);

      LOG(INFO) << "number of rows: " << ret;
      for (int i = 0; i < 20 && i < num_values; ++i) {
        LOG(INFO) << fmt::format("Value {}: {}", i, values[i]);
      }
      // auto curr_page = column_pager->NextPage();
      // for (; curr_page; curr_page = column_pager->NextPage()) {
      //   auto page_type = curr_page->type();
      //   if (curr_page->type() != parquet::PageType::DATA_PAGE &&
      //       curr_page->type() != parquet::PageType::DATA_PAGE_V2) {
      //     LOG(WARNING) << "Unsupported page type: "
      //                  << ParquetPageTypeToString(page_type);
      //     throw std::runtime_error("Unsupported page type.");
      //   }
      //   auto data_page = static_cast<parquet::DataPage *>(curr_page.get());
      //   sort_page(data_page, column_descr);
      // }
    }
    return arrow::Result<std::shared_ptr<arrow::Array>>(nullptr);
  }

  arrow::Result<std::shared_ptr<arrow::Array>> sort_by_column_arrow() {
    if (col_idx_ >= metadata_->num_columns()) {
      LOG(ERROR) << "Column index out of range.";
      return arrow::Status::Invalid("Column index out of range.");
    }

    auto column_descr = metadata_->schema()->Column(col_idx_);
    if (column_descr->physical_type() != DType::type_num) {
      LOG(ERROR) << "Column is not a BYTE_ARRAY column.";
      return arrow::Status::Invalid("Column is not a BYTE_ARRAY column.");
    }

    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      auto row_group = file_reader_->RowGroup(i);
      auto col_reader = row_group->Column(col_idx_);
      auto string_reader =
          std::static_pointer_cast<parquet::ByteArrayReader>(col_reader);

      auto num_values = metadata_->num_rows();
      std::vector<DType::c_type> values(num_values);
      int64_t read_num = 0;
      string_reader->ReadBatch(num_values, nullptr, nullptr, &values[0],
                               &read_num);
      LOG(INFO) << "number of rows: " << read_num;
      for (int i = 0; i < 20 && i < read_num; ++i) {
        LOG(INFO) << fmt::format("Value {}: {}", i, values[i]);
      }
    }
    return arrow::Result<std::shared_ptr<arrow::Array>>(nullptr);
  }

  arrow::Status write(const std::string &output_file) override {
    throw std::runtime_error("Not implemented yet.");
    return arrow::Status::OK();
  }

private:
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

  void sort_page(const parquet::DataPage *data_page,
                 const parquet::ColumnDescriptor *column_descr) {
    auto encoding = data_page->encoding();
    std::unique_ptr<parquet::TypedDecoder<DType>> decoder;
    switch (encoding) {
    case parquet::Encoding::PLAIN:
    case parquet::Encoding::BYTE_STREAM_SPLIT:
    case parquet::Encoding::RLE:
    case parquet::Encoding::DELTA_BINARY_PACKED:
    case parquet::Encoding::DELTA_BYTE_ARRAY:
    case parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY: {
      decoder =
          parquet::MakeTypedDecoder<DType>(encoding, column_descr, nullptr);
      break;
    }

    case parquet::Encoding::RLE_DICTIONARY:
      throw std::runtime_error("Dictionary page must be before data page.");

    default:
      throw std::runtime_error("Unknown encoding type.");
    }

    auto num_values = data_page->num_values();
    // TODO(LH): handle InitializeLevelDecoders
    int levels_byte_size = 0;
    const uint8_t *buffer = data_page->data() + levels_byte_size;
    const int data_size = data_page->size() - levels_byte_size;

    decoder->SetData(static_cast<int>(num_values), buffer, data_size);

    std::vector<DType::c_type> values(num_values);
    decoder->Decode(&values[0], num_values);

    for (int i = 0; i < 20 && i < num_values; ++i) {
      LOG(INFO) << fmt::format("Value {}: {}", i, values[i]);
    }
  }

  unique_ptr<parquet::ParquetFileReader> file_reader_;
  shared_ptr<parquet::FileMetaData> metadata_;
};

} // namespace whippet_sort