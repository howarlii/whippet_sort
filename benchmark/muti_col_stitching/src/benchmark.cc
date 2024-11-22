#include <cstddef>
#include <functional>
#include <iostream>
#include <memory>
#include <ratio>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "benchmark_helper.h"
#include "parquet.h"
#include "stitching_sort.h"
#include "utils.h"

using namespace whippet_sort;

DEFINE_string(input_file,
              std::string(PROJECT_SOURCE_DIR) +
                  "/data/input-20-150-sed0.parquet",
              "Input file path");
DEFINE_double(std_dev_lmt, 0.15, "Standard deviation limit");
DEFINE_int32(num_runs, 1, "number of runs");
DEFINE_int32(warmup, 0, "number of warmup runs");
DEFINE_bool(debug, false, "Debug mode");

DEFINE_int32(num_cols, 3, "number of columns to sort");

DEFINE_bool(std, false, "");
DEFINE_bool(o_by_o, false, "");
DEFINE_bool(stitching_all, false, "");

std::vector<size_t> sort_std(int num_cols) {
  utils::BenchmarkHelper helper("std");
  size_t num_rows;
  std::vector<std::vector<uint32_t>> cols;
  cols.resize(num_cols);

  helper.add_step("read", [&]() {
    auto reader = std::make_unique<ParquetReaderVec>(FLAGS_input_file);
    for (size_t i = 0; i < num_cols; ++i) {
      cols[i] = reader->read_col_int32<uint32_t>(i);
    }
    num_rows = cols[0].size();
    return 0.0;
  });

  std::vector<size_t> idx;
  helper.add_step("sort", [&]() {
    idx.resize(num_rows);
    std::iota(idx.begin(), idx.end(), 0);

    std::sort(idx.begin(), idx.end(), [&](int i, int j) {
      for (size_t k = 0; k < num_cols; ++k) {
        if (cols[k][i] != cols[k][j]) {
          return cols[k][i] < cols[k][j];
        }
      }
      return false;
    });
    return 0.0;
  });

  helper.warmup(FLAGS_warmup);
  helper.run(FLAGS_num_runs);

  std::cout << helper.get_detail_json();
  auto [avg, mid, std_dev] = helper.get_tot_statics();
  std::cout << "# sort_std sorting - Median: " << mid << "ms, Average: " << mid
            << "ms, std_dev: " << std_dev << std::endl;

  auto rate = std_dev / avg;
  if (rate > FLAGS_std_dev_lmt) {
    LOG(ERROR) << "Standard deviation is too high: " << rate;
    exit(1);
  }
  return idx;
}

void sort_1by1(int num_cols) {
  utils::BenchmarkHelper helper("1by1");
  size_t num_rows;
  std::vector<std::vector<uint32_t>> cols;
  cols.resize(num_cols);

  helper.add_step("read", [&]() {
    auto reader = std::make_unique<ParquetReaderVec>(FLAGS_input_file);
    for (size_t i = 0; i < num_cols; ++i) {
      cols[i] = reader->read_col_int32<uint32_t>(i);
    }
    num_rows = cols[0].size();
    return 0.0;
  });

  std::vector<size_t> idx;
  int time_stitching = 0;
  int time_sorting = 0;
  int time_grouping = 0;

  helper.add_step("stitching", [&]() {
    utils::Timer timer;
    time_stitching = time_sorting = time_grouping = 0;

    timer.start();
    idx.resize(num_rows);
    std::iota(idx.begin(), idx.end(), 0);
    timer.stop();
    time_grouping += timer.get_ms();

    std::vector<std::pair<size_t, size_t>> grouping = {{0, num_rows}};
    for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
      auto r = stitch::createStitchingSorterOperator(4);
      r->init(num_rows, &idx, std::move(grouping));

      timer.start();
      r->setData(cols[col_idx]);
      timer.stop();
      time_stitching += timer.get_ms();

      timer.start();
      r->sort();
      timer.stop();
      time_sorting += timer.get_ms();

      timer.start();
      grouping = r->grouping();
      timer.stop();
      time_grouping += timer.get_ms();
    }
    return time_stitching;
  });
  helper.add_step("sorting", [&]() { return time_sorting; });
  helper.add_step("grouping", [&]() { return time_grouping; });

  helper.warmup(FLAGS_warmup);
  helper.run(FLAGS_num_runs);

  std::cout << helper.get_detail_json();
  auto [avg, mid, std_dev] = helper.get_tot_statics();
  std::cout << "# 1by1 sorting - Median: " << mid << "ms, Average: " << mid
            << "ms, std_dev: " << std_dev << std::endl;

  if (FLAGS_debug) {
    for (size_t i = 1; i < num_rows; i++) {
      auto x = idx[i - 1];
      auto y = idx[i];

      for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
        if (cols[col_idx][x] != cols[col_idx][y]) {
          CHECK_LT(cols[col_idx][x], cols[col_idx][y])
              << "col_idx: " << col_idx << "  i: " << i << ", x: " << x
              << ", y: " << y;
          break;
        }
      }
    }
    // std::cout << "Sorting is correct" << std::endl;
    LOG(INFO) << "Sorting is correct";
  }

  auto rate = std_dev / avg;
  if (rate > FLAGS_std_dev_lmt) {
    LOG(ERROR) << "Standard deviation is too high: " << rate;
    exit(1);
  }
}

void sort_stitch_all(int num_cols) {
  utils::BenchmarkHelper helper("stitching_all");
  size_t num_rows;
  std::vector<std::vector<uint32_t>> cols;
  cols.resize(num_cols);

  helper.add_step("read", [&]() {
    auto reader = std::make_unique<ParquetReaderVec>(FLAGS_input_file);
    for (size_t i = 0; i < num_cols; ++i) {
      cols[i] = reader->read_col_int32<uint32_t>(i);
    }
    num_rows = cols[0].size();
    return 0.0;
  });

  std::vector<size_t> idx;
  int time_stitching = 0;
  int time_sorting = 0;
  int time_grouping = 0;

  helper.add_step("stitching", [&]() {
    idx.resize(num_rows);
    std::iota(idx.begin(), idx.end(), 0);

    std::vector<std::pair<size_t, size_t>> grouping = {{0, num_rows}};
    {
      utils::Timer timer;
      time_stitching = time_sorting = time_grouping = 0;

      auto r = stitch::createStitchingSorterOperator(4 * num_cols);
      r->init(num_rows, &idx, std::move(grouping));

      timer.start();
      for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
        r->setData(cols[col_idx]);
      }
      timer.stop();
      time_stitching += timer.get_ms();

      timer.start();
      r->sort();
      timer.stop();
      time_sorting += timer.get_ms();

      timer.start();
      grouping = r->grouping();
      timer.stop();
      time_grouping += timer.get_ms();
    }
    return time_stitching;
  });
  helper.add_step("sorting", [&]() { return time_sorting; });
  helper.add_step("grouping", [&]() { return time_grouping; });

  helper.warmup(FLAGS_warmup);
  helper.run(FLAGS_num_runs);

  std::cout << helper.get_detail_json();
  auto [avg, mid, std_dev] = helper.get_tot_statics();
  std::cout << "# stitching all sorting - Median: " << mid
            << "ms, Average: " << mid << "ms, std_dev: " << std_dev
            << std::endl;

  if (FLAGS_debug) {
    for (size_t i = 1; i < num_rows; i++) {
      auto x = idx[i - 1];
      auto y = idx[i];

      for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
        if (cols[col_idx][x] != cols[col_idx][y]) {
          CHECK_LT(cols[col_idx][x], cols[col_idx][y])
              << "i: " << i << ", x: " << x << ", y: " << y;
          break;
        }
      }
    }
    // std::cout << "Sorting is correct" << std::endl;
    LOG(INFO) << "Sorting is correct";
  }

  auto rate = std_dev / avg;
  if (rate > FLAGS_std_dev_lmt) {
    LOG(ERROR) << "Standard deviation is too high: " << rate;
    exit(1);
  }
}

int main(int argc, char *argv[]) {
  // Parse command line flags
  google::InitGoogleLogging(argv[0]);
  // FLAGS_colorlogtostderr = true;
  // FLAGS_alsologtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  nice(-19);

  // Use the input_file flag
  std::string input_file = FLAGS_input_file;
  size_t num_cols = FLAGS_num_cols;

  std::cout << "# input_file: " << input_file << std::endl;

  bool run_all = !FLAGS_std && !FLAGS_o_by_o && !FLAGS_stitching_all;

  if (FLAGS_std || run_all) {
    sort_std(num_cols);
  }

  if (FLAGS_o_by_o || run_all) {
    sort_1by1(num_cols);
  }

  if (FLAGS_stitching_all || run_all) {
    sort_stitch_all(num_cols);
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}