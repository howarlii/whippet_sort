add_library(trie_sort semistring.cc trie_sort.cc trie_sort.h)
target_link_libraries(trie_sort PRIVATE glog::glog fmt::fmt)

add_library(trie_sort_encode hack_column_reader.h hack_encoding.h)
# target_include_directories(benchmark PRIVATE ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow/include)
# target_link_directories(benchmark PRIVATE ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow/lib)

# Link libraries to the benchmark target
target_link_libraries(
  trie_sort_encode
  PRIVATE Arrow::arrow_static
          Parquet::parquet_static
          Threads::Threads
          glog::glog
          fmt::fmt
)
