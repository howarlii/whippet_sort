cmake_minimum_required(VERSION 3.20)
project(sort_example LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add the Arrow and Parquet paths to the CMake prefix
list(APPEND CMAKE_PREFIX_PATH ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow)
message(STATUS ${CMAKE_PREFIX_PATH})

# Find the necessary packages
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(Threads REQUIRED)

# Define include directories and link directories for the target
add_executable(benchmark benchmark.cc parquet_sorter.cc)
target_include_directories(benchmark PRIVATE ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow/include)
target_link_directories(benchmark PRIVATE ${PROJECT_SOURCE_DIR}/../../third_party/install/arrow/lib)

# Link libraries to the benchmark target
target_link_libraries(benchmark 
    PRIVATE 
    Arrow::arrow_static
    Parquet::parquet_static
    Threads::Threads
    -O3
)
