#include "parquet_sorter.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <queue>
#include <set>

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/encoding.h"
#include "parquet/file_reader.h"

namespace whippet_sort {

arrow::Result<std::shared_ptr<arrow::Array>>
ParquetSorterArrow::sort_by_column() {
  // Sort the column
  arrow::compute::SortOptions sort_options;
  auto ret = arrow::compute::SortIndices(column_, sort_options, &exec_ctx_);

  if (ret.ok()) {
    sort_index_ = ret.ValueOrDie();
  } else {
    LOG(WARNING) << "Failed to sort column " << col_idx_;
  }

  return ret;
}

} // namespace whippet_sort
