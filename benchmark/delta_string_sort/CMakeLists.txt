cmake_minimum_required(VERSION 3.20)
project(sort_example LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Add the Arrow and Parquet paths to the CMake prefix
list(APPEND CMAKE_PREFIX_PATH ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow)
message(STATUS ${CMAKE_PREFIX_PATH})

# Find the necessary packages
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(Threads REQUIRED)
find_package(glog CONFIG REQUIRED)
find_package(Fmt CONFIG REQUIRED)

set(CMAKE_CXX_FLAGS_DEBUG "-g -Wall -Wno-mismatched-tags -Wno-deprecated-declarations -Wno-deprecated-this-capture")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -Wall -fsanitize=address -march=native -DNDEBUG")

# Define include directories and link directories for the target
add_executable(benchmark benchmark.cc parquet_sorter.cc)
target_include_directories(benchmark PRIVATE ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow/include)
target_link_directories(benchmark PRIVATE ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow/lib)

# Link libraries to the benchmark target
target_link_libraries(
  benchmark
  PRIVATE Arrow::arrow_static
          Parquet::parquet_static
          Threads::Threads
          glog::glog
          fmt::fmt
)
