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

namespace whippet_sort {

class Utils {
public:
  template <typename T>
  static std::size_t hashCombine(std::size_t seed, const T &value) {
    return seed ^
           (std::hash<T>()(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2));
  }

  static void drop_file_cache(const std::string &file_path) {
    std::string command =
        "dd of=" + file_path +
        " oflag=nocache conv=notrunc,fdatasync status=none count=0";
    auto drop_cache = system(command.c_str());
    if (drop_cache != 0) {
      std::cerr << "Failed to drop file cache. Error code: " << drop_cache
                << std::endl;
    }
  }
  static void check_column_type(const std::shared_ptr<arrow::Table> &table,
                                int column_index) {
    auto column = table->column(column_index);
    auto type = column->type();

    std::cout << "Column " << column_index << " type: " << type->ToString()
              << std::endl;

    if (type->id() == arrow::Type::DICTIONARY) {
      auto dict_type = std::static_pointer_cast<arrow::DictionaryType>(type);
      std::cout << "  This is a dictionary-encoded column." << std::endl;
      std::cout << "  Index type: " << dict_type->index_type()->ToString()
                << std::endl;
      std::cout << "  Value type: " << dict_type->value_type()->ToString()
                << std::endl;
    } else if (type->id() == arrow::Type::STRING) {
      auto string_type = std::static_pointer_cast<arrow::StringType>(type);
      std::cout << "  This is a string column." << std::endl;
    }
  }

  using BenchmarkStep = std::function<std::chrono::microseconds()>;
  static std::tuple<double, double, double>
  benchmark(const std::string &name, int num_runs,
            std::vector<std::pair<std::string, BenchmarkStep>> &&steps,
            int warmup = 1) {
    std::vector<std::vector<double>> durations(steps.size(),
                                               std::vector<double>(num_runs));
    std::vector<double> tot_durations(num_runs);

    for (int round = -warmup; round < num_runs; ++round) {
      double tot_duration = 0;
      for (int step_i = 0; step_i < steps.size(); ++step_i) {
        auto start = std::chrono::high_resolution_clock::now();
        // auto end = start;
        std::chrono::microseconds time_cost;
        try {
          time_cost = std::get<1>(steps[step_i])();
          if (time_cost.count() == 0)
            time_cost = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start);
        } catch (const std::exception &e) {
          time_cost = std::chrono::microseconds::zero();
          std::cerr << "Error in step " << step_i << ": " << e.what()
                    << std::endl;
        }
        if (round >= 0) {
          double duration = time_cost.count();

          durations[step_i][round] = duration;
          tot_duration += duration;
        }
      }
      if (round >= 0) {
        tot_durations[round] = tot_duration;
      }
    }

    std::vector<std::string> step_names;
    for (auto &step : steps) {
      step_names.push_back(std::get<0>(step));
    }
    std::cout << std::endl << fmt::format("\"{}\":  {}\n", name, "{");
    // Calculate average
    for (int step_i = 0; step_i < steps.size(); ++step_i) {
      double average_us = std::accumulate(durations[step_i].begin(),
                                          durations[step_i].end(), 0.0) /
                          num_runs;
      // Calculate median
      std::sort(durations[step_i].begin(), durations[step_i].end());
      double median_us = durations[step_i][num_runs / 2];
      if (durations[step_i].front() < 1) {
        average_us = 0;
        median_us = 0;
      }
      std::cout << fmt::format("\"{}\": {:.1f}{}", step_names[step_i],
                               median_us / 1000,
                               step_i == steps.size() - 1 ? "" : ",")
                << std::endl;
    }
    std::cout << "},\n";

    double average_us =
        std::accumulate(tot_durations.begin(), tot_durations.end(), 0.0) /
        num_runs;
    // Calculate median
    std::sort(tot_durations.begin(), tot_durations.end());
    double median_us = tot_durations[num_runs / 2];

    double sum_squares = 0;
    for (auto duration : tot_durations) {
      sum_squares += (duration - average_us) * (duration - average_us);
    }
    double std_dev = sqrt(sum_squares / tot_durations.size());
    return {median_us, average_us, std_dev};
  }

  static bool check_whippet_sort_correctness(const std::string &parquet_file,
                                             int sorted_column_index) {
    std::shared_ptr<arrow::io::RandomAccessFile> input_file;
    auto state = arrow::io::ReadableFile::Open(parquet_file);
    if (!state.ok()) {
      std::cerr << "Failed to open input file." << std::endl;
      throw std::runtime_error("Failed to open input parquet file");
    }

    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
        parquet::ParquetFileReader::Open(input_file);

    std::shared_ptr<parquet::FileMetaData> file_metadata =
        parquet_reader->metadata();

    if (sorted_column_index >= file_metadata->num_columns()) {
      std::cerr << "Invalid column index." << std::endl;
      return false;
    }

    // Read the column data
    std::shared_ptr<parquet::ColumnReader> column_reader =
        parquet_reader->RowGroup(0)->Column(sorted_column_index);

    parquet::Int64Reader *int64_reader =
        static_cast<parquet::Int64Reader *>(column_reader.get());

    int64_t values[1000];
    int64_t values_read;
    int64_t previous_value = std::numeric_limits<int64_t>::min();

    while (int64_reader->HasNext()) {
      int64_reader->ReadBatch(1000, nullptr, nullptr, values, &values_read);

      for (int64_t i = 0; i < values_read; ++i) {
        if (values[i] < previous_value) {
          std::cerr << "Column is not sorted at index " << i << std::endl;
          return false;
        }
        previous_value = values[i];
      }
    }

    return true;
  }
};
}; // namespace whippet_sort