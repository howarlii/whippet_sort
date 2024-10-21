#include <functional>
#include <memory>
#include <random>

#include "trie_sort/trie_sort_v2.h"

#include "gtest/gtest.h"

namespace whippet_sort::trie_v2 {
namespace {

class TrieTestV2 : public ::testing::Test {
public:
  void init(uint8_t lmt) { characters = "abcdefghijklmnopqrstuvwxyz"; }

  void generate(int n, int str_max_len) {
    // std::random_device rd;
    std::mt19937 gen(10);
    std::uniform_int_distribution<uint32_t> dist;
    a_prefixs.reserve(n);
    a_prefix_lens.reserve(n);
    int last_len = 0;
    for (int i = 0; i < n; ++i) {
      int prefix_len = dist(gen) % (last_len + 1);
      int len = dist(gen) % (str_max_len - prefix_len + 1);
      if (prefix_len == 0)
        len = std::max(len, 1);
      std::string key = generateRandomString(gen, len);

      a_prefixs.push_back(key);
      a_prefix_lens.push_back(prefix_len);
      last_len = key.size() + prefix_len;
    }

    LOG(INFO) << "generate data, str_num: " << n
              << ", str_max_len: " << str_max_len;
  }

  void stdSort() {
    auto begin_time = std::chrono::steady_clock::now();
    a_decoded = decodePrefixEecode(a_prefixs, a_prefix_lens, enable_debug);
    std::sort(a_decoded.begin(), a_decoded.end(),
              [](auto &x, auto &y) { return x < y; });

    auto end_time = std::chrono::steady_clock::now() - begin_time;
    LOG(INFO) << "decode + std::sort time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_time)
                     .count()
              << "ms";

    if (enable_debug) {
      for (int i = 0; i < a_decoded.size(); ++i) {
        std::cout << a_decoded[i] << "\n";
      }
      std::cout << "==================\n";
    }
  }

  void insertAll() {
    auto begin_time = std::chrono::steady_clock::now();
    for (int i = 0; i < a_prefixs.size(); ++i) {
      trie_.insert(a_prefix_lens[i], a_prefixs[i], i);
    }
    auto end_time = std::chrono::steady_clock::now() - begin_time;
    LOG(INFO) << "insert time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_time)
                     .count()
              << "ms";
  }

  void outputIt() {
    std::vector<std::string> res_a;
    std::vector<int> res_prefix_lens;
    res_a.reserve(trie_.valueNum());
    res_prefix_lens.reserve(trie_.valueNum());

    auto f = [&](size_t prefix_len, std::string key, ValueT value) {
      res_a.emplace_back(std::move(key));
      res_prefix_lens.push_back(prefix_len);
    };

    auto begin_time = std::chrono::steady_clock::now();

    trie_printer = std::make_unique<TriePrinter>(trie_.build());
    trie_printer->preSort();
    trie_printer->registerFunc(f);
    trie_printer->print();

    auto end_time = std::chrono::steady_clock::now() - begin_time;
    LOG(INFO) << "output time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_time)
                     .count()
              << "ms";

    auto out = decodePrefixEecode(res_a, res_prefix_lens, enable_debug);

    for (int i = 0; i < out.size(); ++i) {
      ASSERT_EQ(out[i], a_decoded[i]) << "on line: " << i;
    }
  }

  static std::vector<std::string>
  decodePrefixEecode(const std::vector<std::string> &a,
                     const std::vector<int> &prefix_lens, bool print = false) {
    std::vector<std::string> ans;
    std::string last;
    for (int i = 0; i < a.size(); ++i) {
      last = last.substr(0, prefix_lens[i]) + a[i];

      ans.push_back(last);
    }
    if (print) {
      for (int i = 0; i < a.size(); ++i) {
        std::cout << a[i] << "\n";
      }
      std::cout << "==================\n";
    }
    return ans;
  }

protected:
  std::string generateRandomString(std::mt19937 &gen, int length) {
    std::uniform_int_distribution<> charDist(0, characters.size() - 1);

    std::string randomString;
    for (int i = 0; i < length; ++i) {
      randomString += characters[charDist(gen)];
    }
    return randomString;
  }

  // put in any custom data members that you need
  TrieBuilder trie_;
  std::unique_ptr<TriePrinter> trie_printer;

  std::string characters;
  std::vector<std::string> a_prefixs;
  std::vector<int> a_prefix_lens;
  std::vector<std::string> a_decoded;
  bool enable_debug = false;
};

TEST_F(TrieTestV2, t1) {
  this->init(2);
  enable_debug = true;

  generate(10, 10);
  stdSort();
  insertAll();
  outputIt();
}

TEST_F(TrieTestV2, t2) {
  this->init(8);
  // enable_debug = true;

  generate(1e6, 200);
  stdSort();
  insertAll();
  outputIt();
}

TEST_F(TrieTestV2, t3) {
  this->init(8);
  // enable_debug = true;

  generate(1e7, 500);
  stdSort();
  insertAll();
  outputIt();
}

TEST_F(TrieTestV2, t4) {
  this->init(8);
  // enable_debug = true;

  generate(1e7, 1000);
  stdSort();
  insertAll();
  outputIt();
}
} // namespace
} // namespace whippet_sort::trie_v2