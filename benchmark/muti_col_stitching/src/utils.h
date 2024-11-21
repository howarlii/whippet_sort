#pragma once

#include <algorithm>
#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <cstdlib>
#include <fmt/core.h>
#include <fmt/format.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/encoding.h"
#include "parquet/file_reader.h"
#include "parquet/types.h"

namespace whippet_sort::utils {

class Timer {
public:
  // By default use PROCESS_CPU time to avoid frequency change, power scaling
  // and context switch issues.
  Timer(clockid_t clock_type = CLOCK_PROCESS_CPUTIME_ID)
      : clock_type(clock_type) {}

  void start() { clock_gettime(clock_type, &start_time); }
  void stop() { clock_gettime(clock_type, &end_time); }

  double get_us() {
    return static_cast<double>(end_time.tv_sec - start_time.tv_sec) * 1e6 +
           static_cast<double>(end_time.tv_nsec - start_time.tv_nsec) / 1e3;
  }
  double get_ms() {
    return static_cast<double>(end_time.tv_sec - start_time.tv_sec) * 1e3 +
           static_cast<double>(end_time.tv_nsec - start_time.tv_nsec) / 1e6;
  }

private:
  timespec start_time;
  timespec end_time;
  clockid_t clock_type;
};

template <typename T>
static std::size_t hashCombine(std::size_t seed, const T &value) {
  return seed ^
         (std::hash<T>()(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

[[maybe_unused]] static void drop_file_cache(const std::string &file_path) {
  std::string command =
      "dd of=" + file_path +
      " oflag=nocache conv=notrunc,fdatasync status=none count=0";
  auto drop_cache = system(command.c_str());
  if (drop_cache != 0) {
    std::cerr << "Failed to drop file cache. Error code: " << drop_cache
              << std::endl;
  }
}

} // namespace whippet_sort::utils