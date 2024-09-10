#include <functional>
#include <random>

#include "dict_tree.h"

#include "gtest/gtest.h"

namespace whippet_sort {
namespace {

TEST(SemiStringTest, t1) {
  std::string a("qwe");
  a[0] = 0x12;
  a[1] = 0x23;
  a[2] = 0x45;
  std::string b("qwe");
  a[0] = 0x67;
  a[1] = 0x89;
  a[2] = 0xab;

  SemiStringView sa(a);
  ASSERT_EQ(sa.length(), 6);
  SemiStringView sar = sa.substr_tail(3);
  ASSERT_EQ(sar.length(), 3);
  for (int i = 0; i < sar.length(); i++) {
    ASSERT_EQ(sar[i], sa[i + 3]);
  }
  SemiStringView sal = sa.substr(0, 3);
  ASSERT_EQ(sal.length(), 3);
  for (int i = 0; i < sal.length(); i++) {
    ASSERT_EQ(sal[i], sa[i]);
  }
  SemiString com;
  com.append(sal);
  com.append(sar);
  ASSERT_EQ(com.length(), 6);
  for (int i = 0; i < com.length(); i++) {
    ASSERT_EQ(com[i], sa[i]);
  }

  com.pop_back(3);
  ASSERT_EQ(com.length(), 3);
  for (int i = 0; i < com.length(); i++) {
    ASSERT_EQ(com[i], sa[i]);
  }
}

class DictTreeTest : public ::testing::Test {
public:
  void init(uint8_t lmt) {
    characters.clear();
    for (uint8_t i = 0; i < lmt; ++i) {
      for (uint8_t j = 0; j < lmt; ++j) {
        if (i + j)
          characters += char(i * 16 + j);
      }
    }
  }

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
      std::string key = generateRandomString(gen, len);

      a_prefixs.push_back(key);
      a_prefix_lens.push_back(prefix_len);
      last_len = key.size() + prefix_len;
    }

    a_decoded = decodePrefixEecode(a_prefixs, a_prefix_lens, false);
    std::sort(a_decoded.begin(), a_decoded.end(), std::greater<std::string>());
    for (int i = 0; i < a_decoded.size(); ++i) {
      SemiStringView q((std::string_view(a_decoded[i])));
      for (int j = 0; j < q.length(); ++j) {
        std::cout << (int)q[j] << " ";
      }
      std::cout << "\n";
    }
    std::cout << "==================\n";
  }

  void insertAll() {
    for (int i = 0; i < a_prefixs.size(); ++i) {
      dict_tree.Insert(a_prefix_lens[i], a_prefixs[i], i);
    }
  }

  void outputIt() {

    std::vector<std::string> res_a;
    std::vector<int> res_prefix_lens;

    auto t = dict_tree.build();
    while (t->has_next()) {
      size_t prefix_len;
      std::string key;
      int values;
      t->Next(&prefix_len, &key, &values);
      res_a.push_back(key);
      res_prefix_lens.push_back(prefix_len);
      // std::cout << prefix_len << " " << key << " " << values << std::endl;
    }
    auto out = decodePrefixEecode(res_a, res_prefix_lens, true);

    for (int i = 0; i < out.size(); ++i) {
      ASSERT_EQ(out[i], a_decoded[i]);
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
        SemiStringView q((std::string_view(ans[i])));
        for (int j = 0; j < q.length(); ++j) {
          std::cout << (int)q[j] << " ";
        }
        std::cout << "\n";
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
  DictTreeBuilder dict_tree;

  std::string characters;
  std::vector<std::string> a_prefixs;
  std::vector<int> a_prefix_lens;
  std::vector<std::string> a_decoded;
};

TEST_F(DictTreeTest, t1) {
  this->init(2);

  generate(10, 10);
  insertAll();
  outputIt();
}
} // namespace
} // namespace whippet_sort