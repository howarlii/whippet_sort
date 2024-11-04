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
#include <thread>

DEFINE_string(n_rows, "20", "Number of rows (can be in scientific notation)");
DEFINE_int32(str_len_avg, 150, "Average length of strings");
DEFINE_int32(data_type, 2, "Type of data to generate");
DEFINE_bool(debug, false, "debug mode");

std::random_device rd;
std::mt19937 mt_generator(rd());
const int kNumThreads = std::min<int>(32, std::thread::hardware_concurrency());

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
  random_string.reserve(length);
  for (int i = 0; i < length; ++i) {
    random_string += characters[distribution(mt_generator)];
  }
  return random_string;
}

std::string generate_random_string(std::mt19937 &mt_generator, int bnum,
                                   const std::vector<std::string> &blocks) {
  std::uniform_int_distribution<> distribution(0, blocks.size() - 1);

  std::string random_string;
  random_string.reserve(bnum * blocks.front().size());
  for (int i = 0; i < bnum; ++i) {
    random_string += blocks[distribution(mt_generator)];
  }
  return random_string;
}

arrow::Result<std::shared_ptr<arrow::Array>>
generate_rnd_str_array(int n, int str_avg_len) {
  // Define min and max lengths based on 0.8 * str_avg_len and 1.2 * str_avg_len
  int min_len = static_cast<int>(0.8 * str_avg_len);
  int max_len = static_cast<int>(1.2 * str_avg_len);

  std::vector<std::thread> threads;
  auto num_threads = std::min(kNumThreads, n / 1000 + 1);
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
generate_rnd_pref_str_array(int n, int str_avg_len, float sratio = 0.5) {
  // Define min and max lengths based on 0.8 * str_avg_len and 1.2 * str_avg_len
  int min_len = static_cast<int>(0.8 * str_avg_len);
  int max_len = static_cast<int>(1.2 * str_avg_len);

  std::vector<std::thread> threads;
  auto num_threads = std::min(kNumThreads, n / 1000 + 1);
  std::vector<std::vector<std::string>> thread_strings(num_threads);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 gen(rd() + t);
      std::uniform_int_distribution<> local_length_distribution(min_len,
                                                                max_len);
      std::uniform_real_distribution real_distribution(0.0, 1.0);
      int start = t * n / num_threads;
      int end = (t + 1) * n / num_threads;
      thread_strings[t].reserve(end - start);
      std::string last_str;
      for (int i = start; i < end; ++i) {
        size_t len = local_length_distribution(gen);
        auto k = real_distribution(gen);
        size_t prefix_length =
            std::round(last_str.length() *
                       ((k < sratio) ? (sratio + k / sratio * (1 - sratio))
                                     : ((k - sratio) * sratio / (1 - sratio))));
        prefix_length = std::min(prefix_length, len);
        last_str.resize(prefix_length);
        last_str += generate_random_string(gen, len - prefix_length);
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
generate_block_pref_str_array(int n, int str_avg_len, float sratio = 0.5) {
  int block_len = 10;
  int block_num = 1000;
  // Define min and max lengths based on 0.8 * str_avg_len and 1.2 * str_avg_len
  int min_bnum = static_cast<int>(0.8 * str_avg_len / block_len);
  int max_bnum = static_cast<int>(1.2 * str_avg_len / block_len);

  std::vector<std::string> blocks;
  blocks.reserve(block_num);
  for (int i = 0; i < block_num; ++i) {
    blocks.push_back(generate_random_string(mt_generator, block_len));
  }

  std::vector<std::thread> threads;
  auto num_threads = std::min(kNumThreads, n / 1000 + 1);
  std::vector<std::vector<std::string>> thread_strings(num_threads);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t, &strs = thread_strings[t]]() {
      std::mt19937 gen(rd() + t);
      std::uniform_int_distribution<> local_length_distribution(min_bnum,
                                                                max_bnum);
      std::uniform_real_distribution real_distribution(0.0, 1.0);
      int start = t * n / num_threads;
      int end = (t + 1) * n / num_threads;
      strs.reserve(end - start);
      std::string last_str;
      for (int i = start; i < end; ++i) {
        size_t str_bnum = local_length_distribution(gen);
        auto k = real_distribution(gen);
        size_t prefix_bnum =
            std::round((1.0 * last_str.length() / block_len) *
                       ((k < sratio) ? (sratio + k / sratio * (1 - sratio))
                                     : ((k - sratio) * sratio / (1 - sratio))));
        prefix_bnum = std::min(prefix_bnum, str_bnum);

        last_str.resize(prefix_bnum * block_len);
        last_str += generate_random_string(gen, str_bnum - prefix_bnum, blocks);
        strs.push_back(last_str);
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

std::shared_ptr<arrow::Table> gen_type1(int n, int str_avg_len) {
  std::vector<std::shared_ptr<arrow::Array>> columns(3);
  std::vector<std::thread> threads;

  threads.emplace_back([&]() {
    // random select words to construct string with random prefix length
    columns[0] = generate_block_pref_str_array(n, str_avg_len).ValueOrDie();
  });
  threads.emplace_back([&]() {
    // total random strings
    columns[2] = generate_rnd_str_array(n, str_avg_len).ValueOrDie();
  });
  threads.emplace_back([&]() {
    // random prefix length + random prefix strings
    columns[1] = generate_rnd_pref_str_array(n, str_avg_len).ValueOrDie();
  });

  for (auto &thread : threads) {
    thread.join();
  }
  if (FLAGS_debug) {
    for (auto &col : columns) {
      std::cout << col->ToString() << std::endl;
    }
    return nullptr;
  }

  // Create a schema with one string column
  auto schema = arrow::schema({arrow::field("column0", arrow::large_utf8()),
                               arrow::field("column1", arrow::large_utf8()),
                               arrow::field("column2", arrow::large_utf8())});

  // Create a table from the array
  auto table = arrow::Table::Make(schema, columns);
  return table;
}

std::shared_ptr<arrow::Table> gen_type2(int n, int str_avg_len) {
  std::vector<std::shared_ptr<arrow::Array>> columns(3);
  std::vector<std::thread> threads;
  int col_num = 0;
  threads.emplace_back([&, i = col_num++]() {
    // random prefix length + random prefix strings
    columns[i] = generate_rnd_pref_str_array(n, str_avg_len, 0.2).ValueOrDie();
  });
  threads.emplace_back([&, i = col_num++]() {
    // random prefix length + random prefix strings
    columns[i] = generate_rnd_pref_str_array(n, str_avg_len, 0.5).ValueOrDie();
  });
  threads.emplace_back([&, i = col_num++]() {
    // random prefix length + random prefix strings
    columns[i] = generate_rnd_pref_str_array(n, str_avg_len, 0.8).ValueOrDie();
  });

  for (auto &thread : threads) {
    thread.join();
  }
  if (FLAGS_debug) {
    for (auto &col : columns) {
      std::cout << col->ToString() << std::endl;
    }
    return nullptr;
  }

  // Create a schema with one string column
  auto schema =
      arrow::schema({arrow::field("shared_pref_0.2", arrow::large_utf8()),
                     arrow::field("shared_pref_0.5", arrow::large_utf8()),
                     arrow::field("shared_pref_0.8", arrow::large_utf8())});

  // Create a table from the array
  auto table = arrow::Table::Make(schema, columns);
  return table;
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  int n = scientific_to_int(FLAGS_n_rows);
  int str_avg_len = FLAGS_str_len_avg;

  std::shared_ptr<arrow::Table> table;
  if (FLAGS_data_type == 1)
    table = gen_type1(n, str_avg_len);
  else {
    table = gen_type2(n, str_avg_len);
  }
  if (!table)
    return 0;

  // Output Parquet file
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  auto out_path = std::string(PROJECT_SOURCE_DIR) +
                  fmt::format("/data/input-ty{}-{}-{}.parquet", FLAGS_data_type,
                              FLAGS_n_rows, FLAGS_str_len_avg);
  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(out_path));

  const auto arrow_properties =
      ::parquet::ArrowWriterProperties::Builder().store_schema()->build();

  // Configure Parquet writer properties to use DELTA_BYTE_ARRAY encoding for
  // strings
  parquet::WriterProperties::Builder builder;
  builder.disable_dictionary()
      ->encoding(parquet::Encoding::DELTA_BYTE_ARRAY)
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
