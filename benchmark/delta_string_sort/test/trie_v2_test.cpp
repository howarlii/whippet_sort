#include <functional>
#include <memory>
#include <random>

#include "trie_sort/trie_sort_v2.h"
#include "trie_tester.h"

#include "gtest/gtest.h"

namespace whippet_sort::trie_v2 {
namespace {

class TrieTestV2 : public TrieTester {
public:
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
  TrieBuilder trie_;
  std::unique_ptr<TriePrinter> trie_printer;
};

TEST_F(TrieTestV2, t1) {
  enable_debug = true;

  generate(10, 10);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}

TEST_F(TrieTestV2, t2) {
  // enable_debug = true;

  generate(1e6, 200);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}
TEST_F(TrieTestV2, t2_2) {
  // enable_debug = true;

  generate(1e6, 1600);
  generate(1e6, 1600);
  generate(1e6, 1600);
  generate(1e6, 1600);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}

TEST_F(TrieTestV2, t3) {
  GTEST_SKIP();
  // enable_debug = true;

  generate(1e7, 500);
  gen_done();
  insertAll();
  outputIt();
  check_res();
}

TEST_F(TrieTestV2, t4) {
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