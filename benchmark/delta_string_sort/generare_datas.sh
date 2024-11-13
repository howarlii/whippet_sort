#!/bin/bash

# Array of row counts
row_counts=("2e7")

# Array of string lengths
string_lengths=(1600)

# Array of seeds
seeds=(19260817 114514 1919810)

# gen_cmd="python3 data_generator.py -n {1} -l {2}"
gen_cmd="./build/data_generator --n_rows {1} --str_len_avg {2} --seed {3} "

# Loop through row counts
parallel --jobs 2 echo "Generating data with {1} rows and average string length of {2} and the seed of {3}" \; $gen_cmd ::: "${row_counts[@]}" ::: "${string_lengths[@]}" ::: "${seeds[@]}"

echo "Data generation complete."
