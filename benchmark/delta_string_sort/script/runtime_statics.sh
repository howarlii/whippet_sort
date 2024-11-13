#!/bin/bash

valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/input-ty2-2e6-20-sed0.parquet --sort_col_idx=1 --num_runs=1 --trie
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/input-ty2-2e6-20-sed0.parquet --sort_col_idx=1 --num_runs=1 --hack_arrow

valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/input-ty2-2e6-1600-sed0.parquet --sort_col_idx=1 --num_runs=1 --trie
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/input-ty2-2e6-1600-sed0.parquet --sort_col_idx=1 --num_runs=1 --hack_arrow

valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/input-ty2-2e6-20-sed0.parquet --sort_col_idx=1 --num_runs=1 --trie_v2
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/input-ty2-2e6-1600-sed0.parquet --sort_col_idx=1 --num_runs=1 --trie_v2

valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/input-ty2-2e6-20-sed0.parquet --sort_col_idx=1 --num_runs=1 --arrow
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/input-ty2-2e6-1600-sed0.parquet --sort_col_idx=1 --num_runs=1 --arrow
