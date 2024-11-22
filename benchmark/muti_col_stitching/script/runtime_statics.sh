#!/bin/bash

valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --num_cols=3 --std
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --num_cols=3 --o_by_o
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --num_cols=3 --stitching_all

valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --num_cols=5 --std
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --num_cols=5 --o_by_o
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --num_cols=5 --stitching_all
