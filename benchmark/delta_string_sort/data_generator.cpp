#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <filesystem>
#include <iostream>
#include <parquet/arrow/writer.h>
#include <random>
#include <string>

#include <fmt/core.h>
#include <gflags/gflags.h>
#include <thread>

DEFINE_string(n_rows, "2e5", "Number of rows (can be in scientific notation)");
DEFINE_int32(str_len_avg, 100, "Average length of strings");

std::random_device rd;
std::mt19937 mt_generator(rd());
const int num_threads = std::min<int>(32, std::thread::hardware_concurrency());

// Function to convert scientific notation string to int
int scientific_to_int(const std::string &s) {
  return static_cast<int>(std::stod(s));
}

// Function to generate a random string of a given length
std::string generate_random_string(std::mt19937 &mt_generator, int length) {
  static const std::string characters =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::uniform_int_distribution<> distribution(0, characters.size() - 1);

  std::string random_string;
  for (int i = 0; i < length; ++i) {
    random_string += characters[distribution(mt_generator)];
  }
  return random_string;
}

std::string
generate_random_string(std::mt19937 &mt_generator, int length,
                       const std::vector<std::string> &block_lengths) {
  std::uniform_int_distribution<> distribution(0, block_lengths.size() - 1);

  std::string random_string;
  for (int i = 0; i < length; ++i) {
    random_string += block_lengths[distribution(mt_generator)];
  }
  return random_string;
}

arrow::Result<std::shared_ptr<arrow::Array>>
generate_rnd_str_array(int n, int str_avg_len) {
  // Define min and max lengths based on 0.8 * str_avg_len and 1.2 * str_avg_len
  int min_len = static_cast<int>(0.8 * str_avg_len);
  int max_len = static_cast<int>(1.2 * str_avg_len);

  std::vector<std::thread> threads;
  std::vector<std::vector<std::string>> thread_strings(num_threads);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 gen(rd() + t);
      std::uniform_int_distribution<> local_length_distribution(min_len,
                                                                max_len);
      int start = t * n / num_threads;
      int end = (t + 1) * n / num_threads;
      thread_strings[t].reserve(end - start);
      for (int i = start; i < end; ++i) {
        int random_length = local_length_distribution(gen);
        thread_strings[t].push_back(generate_random_string(gen, random_length));
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Create an Arrow Array of strings
  arrow::LargeStringBuilder string_builder;
  for (auto &thread_string : thread_strings) {
    for (auto &str : thread_string) {
      ARROW_RETURN_NOT_OK(string_builder.Append(std::move(str)));
    }
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(string_builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Array>>
generate_rnd_pref_str_array(int n, int str_avg_len) {
  // Define min and max lengths based on 0.8 * str_avg_len and 1.2 * str_avg_len
  int min_len = static_cast<int>(0.8 * str_avg_len);
  int max_len = static_cast<int>(1.2 * str_avg_len);

  std::vector<std::thread> threads;
  std::vector<std::vector<std::string>> thread_strings(num_threads);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 gen(rd() + t);
      std::uniform_int_distribution<> local_length_distribution(min_len,
                                                                max_len);
      std::uniform_int_distribution<int> int_distribution;
      int start = t * n / num_threads;
      int end = (t + 1) * n / num_threads;
      thread_strings[t].reserve(end - start);
      std::string last_str;
      for (int i = start; i < end; ++i) {
        size_t len = local_length_distribution(gen);
        int prefix_length =
            int_distribution(gen) % (std::min(len, last_str.length()) + 1);
        last_str = last_str.substr(0, prefix_length) +
                   generate_random_string(gen, len - prefix_length);
        thread_strings[t].push_back(last_str);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Create an Arrow Array of strings
  arrow::LargeStringBuilder string_builder;
  for (auto &thread_string : thread_strings) {
    for (auto &str : thread_string) {
      ARROW_RETURN_NOT_OK(string_builder.Append(std::move(str)));
    }
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(string_builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Array>>
generate_block_pref_str_array(int n, int str_avg_len) {
  int block_len = 10;
  int block_num = 1000;
  // Define min and max lengths based on 0.8 * str_avg_len and 1.2 * str_avg_len
  int min_len = static_cast<int>(0.8 * str_avg_len / block_len);
  int max_len = static_cast<int>(1.2 * str_avg_len / block_len);

  std::vector<std::string> block_lengths;
  block_lengths.reserve(block_num);
  for (int i = 0; i < block_num; ++i) {
    block_lengths.push_back(generate_random_string(mt_generator, block_len));
  }

  std::vector<std::thread> threads;
  std::vector<std::vector<std::string>> thread_strings(num_threads);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 gen(rd() + t);
      std::uniform_int_distribution<> local_length_distribution(min_len,
                                                                max_len);
      std::uniform_int_distribution<int> int_distribution;
      int start = t * n / num_threads;
      int end = (t + 1) * n / num_threads;
      thread_strings[t].reserve(end - start);
      std::string last_str;
      for (int i = start; i < end; ++i) {
        size_t len = local_length_distribution(gen);
        int prefix_length =
            int_distribution(gen) % (std::min(len, last_str.length()) + 1);
        last_str =
            last_str.substr(0, prefix_length) +
            generate_random_string(gen, len - prefix_length, block_lengths);
        thread_strings[t].push_back(last_str);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Create an Arrow Array of strings
  arrow::LargeStringBuilder string_builder;
  for (auto &thread_string : thread_strings) {
    for (auto &str : thread_string) {
      ARROW_RETURN_NOT_OK(string_builder.Append(std::move(str)));
    }
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(string_builder.Finish(&array));
  return array;
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  int n = scientific_to_int(FLAGS_n_rows);
  int str_avg_len = FLAGS_str_len_avg;

  std::vector<std::shared_ptr<arrow::Array>> columns(3);
  std::vector<std::thread> threads;

  threads.emplace_back([&]() {
    columns[0] = generate_rnd_str_array(n, str_avg_len).ValueOrDie();
  });
  threads.emplace_back([&]() {
    columns[1] = generate_rnd_pref_str_array(n, str_avg_len).ValueOrDie();
  });
  threads.emplace_back([&]() {
    columns[2] = generate_block_pref_str_array(n, str_avg_len).ValueOrDie();
  });

  for (auto &thread : threads) {
    thread.join();
  }

  // Create a schema with one string column
  auto schema = arrow::schema({arrow::field("column0", arrow::large_utf8()),
                               arrow::field("column1", arrow::large_utf8()),
                               arrow::field("column2", arrow::large_utf8())});

  // Create a table from the array
  auto table = arrow::Table::Make(schema, columns);

  // Output Parquet file
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  auto out_path =
      std::string(PROJECT_SOURCE_DIR) +
      fmt::format("/data/input-{}-{}.parquet", FLAGS_n_rows, FLAGS_str_len_avg);
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(out_path));

  // Configure Parquet writer properties to use DELTA_BYTE_ARRAY encoding for
  // strings
  parquet::WriterProperties::Builder builder;
  builder.disable_dictionary();
  builder.encoding(parquet::Encoding::DELTA_BYTE_ARRAY);
  // builder.encoding(0, parquet::Encoding::DELTA_BYTE_ARRAY);

  std::shared_ptr<parquet::WriterProperties> properties = builder.build();

  // Write the table to the Parquet file using the specified properties
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile,
      parquet::DEFAULT_MAX_ROW_GROUP_LENGTH, properties));

  std::cout << "Parquet file created: " << out_path << ".  file size: "
            << std::filesystem::file_size(out_path) / 1024 / 1024 << "MB"
            << std::endl;

  gflags::ShutDownCommandLineFlags();
  return 0;
}
