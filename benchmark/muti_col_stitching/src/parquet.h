#pragma once

#include <cmath>
#include <deque>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/exec.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <glog/logging.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/encoding.h>
#include <parquet/file_reader.h>

namespace whippet_sort {

inline void ARROW_ABORT_NOTOK(const arrow::Status &st) {
  if (!st.ok()) {
    LOG(FATAL) << "Error: " << st.message();
  }
}

// typedef uint32_t IndexType;
class ParquetReaderVec {
public:
  ParquetReaderVec(std::string input_file)
      : input_file_(std::move(input_file)) {
    ARROW_ABORT_NOTOK(open_file());
  }

  std::vector<int> read_col_int32(int col_idx) {
    std::shared_ptr<arrow::ChunkedArray> column;
    ARROW_ABORT_NOTOK(reader_->ReadColumn(col_idx, &column));
    auto num_rows = column->length();

    DLOG(INFO) << "column type: " << column->type()->ToString();

    std::vector<int> res;
    res.reserve(num_rows);
    for (size_t i = 0; i < column->num_chunks(); ++i) {
      auto chunk = column->chunk(i);
      auto int_chunk =
          std::dynamic_pointer_cast<arrow::Int32Array>(chunk)->raw_values();
      CHECK(int_chunk) << "Failed to cast to Int32Array, col_idx " << col_idx
                       << "  column type: " << column->type()->ToString();
      for (size_t j = 0; j < chunk->length(); ++j) {
        res.push_back(int_chunk[j]);
      }
    }
    return res;
  }

private:
  arrow::Status open_file() {
    ARROW_ASSIGN_OR_RAISE(auto infile,
                          arrow::io::ReadableFile::Open(input_file_));

    // Create a ParquetFileReader
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(
        infile, arrow::default_memory_pool(), &reader_));

    return arrow::Status::OK();
  }

  static std::string ParquetPageTypeToString(parquet::PageType::type type) {
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

  std::string input_file_;
  std::unique_ptr<parquet::arrow::FileReader> reader_;

  //   arrow::compute::ExecContext exec_ctx_;
};

} // namespace whippet_sort