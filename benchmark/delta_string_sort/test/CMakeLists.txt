# Add test cpp file
add_executable(UnitTests parquet_sort.cpp trie_test.cpp)
target_include_directories(UnitTests PRIVATE ${PROJECT_SOURCE_DIR}/src)

# Link test executable against gtest & gtest_main
target_link_libraries(
  UnitTests
  PRIVATE trie_sort
          parquet_sorter
          Threads::Threads
          # GTest::gtest_main GTest::gtest
          gtest
          gtest_main
          glog::glog
          fmt::fmt
)
add_test(UnitTests UnitTests)
