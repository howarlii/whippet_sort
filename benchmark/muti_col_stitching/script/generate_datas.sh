#!/bin/bash

# Array of row counts
row_counts=("2e6" "2e7" "2e8" "2e9")
row_counts=("2e7" "2e8")

# Array of seeds
seeds=(0 19260817 114514 1919810)

# Data types
data_types=(1 2)
data_types=(2)

# value range 2^x
value_ranges=(3 7 10 30)
value_ranges=(7)

# gen_cmd="python3 data_generator.py -n {1} -l {2}"
gen_cmd="./build/data_generator --n_rows {1} --seed {2} --data_type {3} --value_range_2pow {4}"

# Loop through row counts
parallel --jobs 16 echo "Generating data with {1} rows and average string length of {2} and the seed of {3}" \; $gen_cmd ::: "${row_counts[@]}" ::: "${seeds[@]}" ::: "${data_types[@]}" ::: "${value_ranges[@]}"

echo "Data generation complete."
