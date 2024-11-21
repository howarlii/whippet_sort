# Define include directories and link directories for the target
add_executable(benchmark benchmark.cc)
target_include_directories(
  benchmark
  PRIVATE ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow/include)
target_link_directories(
  benchmark PRIVATE ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow/lib)

# Link libraries to the benchmark target
target_link_libraries(
  benchmark PRIVATE Arrow::arrow_static Parquet::parquet_static
                    Threads::Threads glog::glog fmt::fmt)
