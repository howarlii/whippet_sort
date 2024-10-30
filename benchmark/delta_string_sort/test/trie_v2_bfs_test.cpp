#include <functional>
#include <memory>
#include <random>

#include "trie_sort/trie_sort_v2.h"
#include "trie_tester.h"
#include "gtest/gtest.h"

namespace whippet_sort::trie_v2 {
namespace {

class TrieTestV2Bfs : public TrieTester {
public:
  void insertAll() {
    auto begin_time = std::chrono::steady_clock::now();
    std::vector<std::tuple<size_t, std::string_view, int>> keys;
    keys.reserve(a_prefixs.size());
    for (int i = 0; i < a_prefixs.size(); ++i) {
      if (a_prefix_lens[i] == 0 && !keys.empty()) {
        trie_.insert(std::move(keys));
        keys.clear();
      }
      keys.emplace_back(a_prefix_lens[i], a_prefixs[i], i);
    }
    if (!keys.empty())
      trie_.insert(std::move(keys));

    auto end_time = std::chrono::steady_clock::now() - begin_time;
    LOG(INFO) << "insert time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_time)
                     .count()
              << "ms";
  }

  void outputIt() {
    res_pref.reserve(trie_.valueNum());
    res_prefix_lens.reserve(trie_.valueNum());
    a_values.reserve(trie_.valueNum());

    auto f = [&](size_t prefix_len, std::string key, ValueT value) {
      res_pref.emplace_back(std::move(key));
      res_prefix_lens.push_back(prefix_len);
      a_values.push_back(value);
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
  }

protected:
  // put in any custom data members that you need
  TrieBuilderBfs trie_;
  std::unique_ptr<TriePrinter> trie_printer;
};

TEST_F(TrieTestV2Bfs, t1) {
  // enable_debug = true;
  trie_ = TrieBuilderBfs(TrieConfig{.lazy_key_burst_lmt = 3});
  generate(10, 10);
  generate(10, 10);
  generate(10, 10);
  generate(10, 10);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}

TEST_F(TrieTestV2Bfs, t2) {

  generate(1e6, 200);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}

TEST_F(TrieTestV2Bfs, t2_2) {

  // enable_debug = true;

  // trie_ = TrieBuilderBfs(TrieConfig{.lazy_key_burst_lmt = 0});
  generate(1e6, 1600);
  generate(1e6, 1600);
  generate(1e6, 1600);
  generate(1e6, 1600);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}

TEST_F(TrieTestV2Bfs, t3) {
  GTEST_SKIP();
  // enable_debug = true;

  generate(1e7, 500);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}

TEST_F(TrieTestV2Bfs, t4) {
  GTEST_SKIP();
  // enable_debug = true;

  generate(1e7, 1000);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}
} // namespace
} // namespace whippet_sort::trie_v2