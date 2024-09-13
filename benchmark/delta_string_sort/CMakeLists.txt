cmake_minimum_required(VERSION 3.20)
project(sort_example LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(DEBUG_WITH_ASAN "Enable address sanitizer in debug compile mode" ON)
option(ENABLE_GPERF "Enable gperf debug compile mode" OFF)

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
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -Wall -march=native -DNDEBUG")

if(DEBUG_WITH_ASAN)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
endif()

if(ENABLE_GPERF)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -pg")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -pg")
endif()

set(DEPS_INSTALL ${PROJECT_SOURCE_DIR}/../../third_party/install)
set(ARROW_INSTALL ${DEPS_INSTALL}/arrow)
include_directories(SYSTEM ${DEPS_INSTALL}/include ${ARROW_INSTALL}/include)
link_directories(${DEPS_INSTALL}/lib ${ARROW_INSTALL}/lib)

# Setup testing
enable_testing()
find_package(GTest CONFIG REQUIRED)
# add_library(GTest::GTest INTERFACE IMPORTED) target_link_libraries(GTest::GTest INTERFACE gtest_main gtest)

message(STATUS "CXX Flag RELWITHDEBINFO: ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
message(STATUS "CXX Flag DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")

add_compile_definitions(PROJECT_SOURCE_DIR=\"${PROJECT_SOURCE_DIR}\")

add_subdirectory(src)
add_subdirectory(test)
