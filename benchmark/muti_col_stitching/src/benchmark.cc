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

DEFINE_bool(std, false, "");
DEFINE_bool(o_by_o, false, "");
DEFINE_bool(stitching_all, false, "");
// DEFINE_bool(arrow, false, "Run low-level Arrow sorting benchmark");
// DEFINE_bool(trie, false, "Run trie-based sorting benchmark");
// DEFINE_bool(trie_v2, false, "Run trie-based sorting benchmark v2");
// DEFINE_bool(trie_v2_bfs, false, "Run trie-based sorting benchmark v2 bfs");

std::vector<size_t> sort_std() {
  utils::BenchmarkHelper helper("std");
  size_t num_rows;
  std::vector<int> col0, col1, col2;

  helper.add_step("read", [&]() {
    auto reader = std::make_unique<ParquetReaderVec>(FLAGS_input_file);
    col0 = reader->read_col_int32(0);
    col1 = reader->read_col_int32(1);
    col2 = reader->read_col_int32(2);
    num_rows = col0.size();
    return 0.0;
  });

  std::vector<size_t> idx;
  helper.add_step("sort", [&]() {
    std::iota(idx.begin(), idx.end(), 0);

    std::sort(idx.begin(), idx.end(), [&](int i, int j) {
      if (col0[i] != col0[j]) {
        return col0[i] < col0[j];
      }
      if (col1[i] != col1[j]) {
        return col1[i] < col1[j];
      }
      return col2[i] < col2[j];
    });
    return 0.0;
  });

  helper.warmup(FLAGS_warmup);
  helper.run(FLAGS_num_runs);

  std::cout << helper.get_detail_json();
  auto [avg, mid, std_dev] = helper.get_tot_statics();
  std::cout << "# sort_std sorting - Median: " << mid << "ms, Average: " << mid
            << "ms, std_dev: " << std_dev << std::endl;

  return idx;
}

void sort_1by1() {
  utils::BenchmarkHelper helper("1by1");
  size_t num_rows;
  std::vector<int> col0, col1, col2;

  helper.add_step("read", [&]() {
    auto reader = std::make_unique<ParquetReaderVec>(FLAGS_input_file);
    col0 = reader->read_col_int32(0);
    col1 = reader->read_col_int32(1);
    col2 = reader->read_col_int32(2);
    num_rows = col0.size();
    return 0.0;
  });

  std::vector<size_t> idx;
  int time_stitching = 0;
  int time_sorting = 0;
  int time_grouping = 0;

  helper.add_step("sort", [&]() {
    utils::Timer timer;
    time_stitching = time_sorting = time_grouping = 0;

    idx.resize(num_rows);
    std::iota(idx.begin(), idx.end(), 0);

    std::vector<std::pair<size_t, size_t>> grouping = {{0, num_rows}};
    {
      stitch::StitchingSorterOperator<4> r(num_rows, idx, std::move(grouping));

      timer.start();
      r.setData(col0);
      timer.stop();
      time_stitching += timer.get_ms();

      timer.start();
      r.sort();
      timer.stop();
      time_sorting += timer.get_ms();

      timer.start();
      grouping = r.grouping();
      timer.stop();
      time_grouping += timer.get_ms();
    }
    {
      stitch::StitchingSorterOperator<4> r(num_rows, idx, std::move(grouping));

      timer.start();
      r.setData(col1);
      timer.stop();
      time_stitching += timer.get_ms();

      timer.start();
      r.sort();
      timer.stop();
      time_sorting += timer.get_ms();

      timer.start();
      grouping = r.grouping();
      timer.stop();
      time_grouping += timer.get_ms();
    }
    {
      stitch::StitchingSorterOperator<4> r(num_rows, idx, std::move(grouping));

      timer.start();
      r.setData(col2);
      timer.stop();
      time_stitching += timer.get_ms();

      timer.start();
      r.sort();
      timer.stop();
      time_sorting += timer.get_ms();

      timer.start();
      grouping = r.grouping();
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

      if (col0[x] != col0[y]) {
        CHECK_LT(col0[x], col0[y])
            << "i: " << i << ", x: " << x << ", y: " << y;
      } else if (col1[x] != col1[y]) {
        CHECK_LT(col1[x], col1[y])
            << "i: " << i << ", x: " << x << ", y: " << y;
      } else {
        CHECK_LE(col2[x], col2[y])
            << "i: " << i << ", x: " << x << ", y: " << y;
      }
    }
    // std::cout << "Sorting is correct" << std::endl;
    LOG(INFO) << "Sorting is correct";
  }
}

void sort_stitch_all() {
  utils::BenchmarkHelper helper("stitching_all");
  size_t num_rows;
  std::vector<int> col0, col1, col2;

  helper.add_step("read", [&]() {
    auto reader = std::make_unique<ParquetReaderVec>(FLAGS_input_file);
    col0 = reader->read_col_int32(0);
    col1 = reader->read_col_int32(1);
    col2 = reader->read_col_int32(2);
    num_rows = col0.size();
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

      stitch::StitchingSorterOperator<4 * 3> r(num_rows, idx,
                                               std::move(grouping));
      timer.start();
      r.setData(col0);
      r.setData(col1);
      r.setData(col2);
      timer.stop();
      time_stitching += timer.get_ms();

      timer.start();
      r.sort();
      timer.stop();
      time_sorting += timer.get_ms();

      timer.start();
      grouping = r.grouping();
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

      if (col0[x] != col0[y]) {
        CHECK_LT(col0[x], col0[y])
            << "i: " << i << ", x: " << x << ", y: " << y;
      } else if (col1[x] != col1[y]) {
        CHECK_LT(col1[x], col1[y])
            << "i: " << i << ", x: " << x << ", y: " << y;
      } else {
        CHECK_LE(col2[x], col2[y])
            << "i: " << i << ", x: " << x << ", y: " << y;
      }
    }
    // std::cout << "Sorting is correct" << std::endl;
    LOG(INFO) << "Sorting is correct";
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
  std::cout << "# input_file: " << input_file << std::endl;

  bool run_all = !FLAGS_std && !FLAGS_o_by_o && !FLAGS_stitching_all;

  if (FLAGS_std || run_all) {
    sort_std();
  }

  if (FLAGS_o_by_o || run_all) {
    sort_1by1();
  }

  if (FLAGS_stitching_all || run_all) {
    sort_stitch_all();
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}