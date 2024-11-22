#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <filesystem>
#include <iostream>
#include <memory>
#include <parquet/arrow/writer.h>
#include <random>
#include <string>

#include <fmt/core.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <thread>

DEFINE_string(n_rows, "20", "Number of rows (can be in scientific notation)");
// DEFINE_int32(str_len_avg, 150, "Average length of strings");
DEFINE_int32(data_type, 1, "Type of data to generate");
DEFINE_int32(seed, 0, "random seed");
DEFINE_bool(debug, false, "debug mode");

std::random_device rd;
std::mt19937 mt_generator(rd());
const int kNumThreads = std::min<int>(32, std::thread::hardware_concurrency());

// Function to convert scientific notation string to int
size_t scientific_to_int(const std::string &s) {
  return static_cast<size_t>(std::stod(s));
}

arrow::Result<std::shared_ptr<arrow::Array>> generate_rnd_int_array(size_t n,
                                                                    int range) {
  std::vector<std::thread> threads;
  auto num_threads = std::min<size_t>(kNumThreads, n / 1e5 + 1);
  std::vector<int> result(n);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 gen((rd() + t) ^ FLAGS_seed);
      std::uniform_int_distribution<> local_length_distribution(0, range);
      size_t start = t * n / num_threads;
      size_t end = (t + 1) * n / num_threads;
      for (size_t i = start; i < end; ++i) {
        result[i] = local_length_distribution(gen);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Create an Arrow Array of strings
  arrow::Int32Builder builder;

  ARROW_RETURN_NOT_OK(builder.AppendValues(result));

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

std::shared_ptr<arrow::Table> gen_type1(size_t n) {
  const int range = 1 << 10;
  const int num_cols = 5;

  std::vector<std::shared_ptr<arrow::Array>> columns(num_cols);
  std::vector<std::thread> threads;
  for (auto &col : columns) {
    threads.emplace_back(
        [&]() { col = generate_rnd_int_array(n, range).ValueOrDie(); });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  if (FLAGS_debug) {
    for (auto &col : columns) {
      std::cout << col->ToString() << std::endl;
    }
    return nullptr;
  }

  arrow::FieldVector fields;
  for (size_t i = 0; i < num_cols; ++i) {
    fields.push_back(arrow::field(fmt::format("column{}", i), arrow::int32()));
  }
  // Create a schema with one string column
  auto schema = arrow::schema(fields);

  // Create a table from the array
  auto table = arrow::Table::Make(schema, columns);
  return table;
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  auto n = scientific_to_int(FLAGS_n_rows);

  std::shared_ptr<arrow::Table> table;
  if (FLAGS_data_type == 1)
    table = gen_type1(n);
  else {
    CHECK(false) << "Not implemented";
  }
  if (!table)
    return 0;

  // Output Parquet file
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  auto out_path =
      // std::string(PROJECT_SOURCE_DIR) +"/data/"
      "/data/parquet_sorting/" + fmt::format("int32-ty{}-{}-sed{}.parquet",
                                             FLAGS_data_type, FLAGS_n_rows,
                                             FLAGS_seed);
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(out_path));

  const auto arrow_properties =
      ::parquet::ArrowWriterProperties::Builder().store_schema()->build();

  // Configure Parquet writer properties to use DELTA_BYTE_ARRAY encoding for
  // strings
  parquet::WriterProperties::Builder builder;
  builder
      .disable_dictionary()
      // ->encoding(parquet::Encoding::DELTA_BYTE_ARRAY)
      ->compression(parquet::Compression::SNAPPY);
  // builder.encoding(0, parquet::Encoding::DELTA_BYTE_ARRAY);
  const auto parquet_properties = builder.build();

  std::cout << "the type " << table->column(0)->type()->ToString(true)
            << std::endl;

  // Write the table to the Parquet file using the specified properties
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile,
                                 parquet::DEFAULT_MAX_ROW_GROUP_LENGTH,
                                 parquet_properties, arrow_properties));

  std::cout << "Parquet file created: " << out_path << ".  file size: "
            << std::filesystem::file_size(out_path) / 1024 / 1024 << "MB"
            << std::endl;

  gflags::ShutDownCommandLineFlags();
  return 0;
}
