#include <functional>
#include <future>
#include <memory>
#include <random>

#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

class TrieTester : public ::testing::Test {
public:
  void generate(int n, int str_max_len) {
    auto f = std::async(std::launch::async, [=, this]() {
      return generate_imp(n, str_max_len, str_num);
    });
    datagen_futures.push_back(std::move(f));
    str_num += n;
  }

  void gen_done() {
    for (auto &f : datagen_futures) {
      auto [strs, pre_lens] = f.get();
      a_prefixs.insert(a_prefixs.end(), strs.begin(), strs.end());
      a_prefix_lens.insert(a_prefix_lens.end(), pre_lens.begin(),
                           pre_lens.end());
    }

    auto begin_time = std::chrono::steady_clock::now();
    a_original = decodePrefixEecode(a_prefixs, a_prefix_lens, enable_debug);
    a_sorted.reserve(a_original.size());
    for (int i = 0; i < a_original.size(); ++i) {
      a_sorted.emplace_back(i, a_original[i]);
    }
    std::sort(a_sorted.begin(), a_sorted.end(),
              [](auto &x, auto &y) { return x.second < y.second; });

    auto end_time = std::chrono::steady_clock::now() - begin_time;
    LOG(INFO) << "decode + std::sort time: "
              << std::chrono::duration_cast<std::chrono::microseconds>(end_time)
                     .count()
              << "ms";

    if (enable_debug) {
      for (int i = 0; i < a_sorted.size(); ++i) {
        std::cout << fmt::format("{}: {}", a_sorted[i].first,
                                 a_sorted[i].second)
                  << "\n";
      }
      std::cout << "==================\n";
    }
  }

  void check_res() {
    for (int i = 1; i < a_values.size(); ++i) {
      ASSERT_EQ(a_original[a_values[i]], a_sorted[i].second)
          << "on line: " << i << " a_values[i]:" << a_values[i];
    }
    if (index_only)
      return;

    auto out = decodePrefixEecode(res_pref, res_prefix_lens, enable_debug);

    for (int i = 1; i < out.size(); ++i) {
      ASSERT_LE(out[i - 1], out[i]) << "on line: " << i;
    }

    ASSERT_EQ(out.size(), a_sorted.size());
    for (int i = 0; i < out.size(); ++i) {
      ASSERT_EQ(a_sorted[i].second, out[i])
          << fmt::format("on line: {}, values: trie/std: {}/{}", i, a_values[i],
                         a_sorted[i].first);
    }
  }

  static std::vector<std::string>
  decodePrefixEecode(const std::vector<std::string> &a,
                     const std::vector<int> &prefix_lens, bool print = false) {
    std::vector<std::string> ans;
    ans.reserve(a.size());
    std::string last;
    for (int i = 0; i < a.size(); ++i) {
      last = last.substr(0, prefix_lens[i]) + a[i];

      ans.push_back(last);
    }
    if (print) {
      for (int i = 0; i < ans.size(); ++i) {
        std::cout << ans[i] << "\n";
      }
      std::cout << "==================\n";
    }
    return ans;
  }

protected:
  static std::string generateRandomString(std::mt19937 &gen, int length) {
    const std::string characters = "abcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<> charDist(0, characters.size() - 1);

    std::string randomString;
    for (int i = 0; i < length; ++i) {
      randomString += characters[charDist(gen)];
    }
    return randomString;
  }
  static std::pair<std::vector<std::string>, std::vector<int>>
  generate_imp(int n, int str_max_len, int seed) {
    std::vector<std::string> a_prefixs;
    std::vector<int> a_prefix_lens;
    a_prefixs.resize(n);
    a_prefix_lens.resize(n);
    std::mt19937 gen(seed);
    std::uniform_int_distribution<uint32_t> dist;
    int last_len = 0;
    for (int i = 0; i < n; ++i) {
      int prefix_len = dist(gen) % (last_len + 1);
      int len = dist(gen) % (str_max_len - prefix_len + 1);
      if (prefix_len == 0)
        len = std::max(len, 1);
      std::string key = generateRandomString(gen, len);

      last_len = key.size() + prefix_len;
      a_prefixs[i] = std::move(key);
      a_prefix_lens[i] = (prefix_len);
    }

    LOG(INFO) << "generate data, str_num: " << n
              << ", str_max_len: " << str_max_len;
    return {a_prefixs, a_prefix_lens};
  }

  std::vector<
      std::future<std::pair<std::vector<std::string>, std::vector<int>>>>
      datagen_futures;

  std::string characters;
  int str_num = 0;
  std::vector<std::string> a_prefixs;
  std::vector<int> a_prefix_lens;

  bool index_only = false;
  std::vector<int> a_values;
  std::vector<std::string> a_original;
  std::vector<std::pair<int, std::string>> a_sorted;

  std::vector<std::string> res_pref;
  std::vector<int> res_prefix_lens;
  bool enable_debug = false;
};