#!/bin/bash

valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --std
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --o_by_o
valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes ./build/src/benchmark --input_file=/data/parquet_sorting/int32-ty1-2e7-sed0.parquet --warmup=0 --num_runs=1 --stitching_all
