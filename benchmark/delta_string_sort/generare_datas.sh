#!/bin/bash

# Array of row counts
row_counts=("2e5" "2e6" "2e7")

# Array of string lengths
string_lengths=(100 200 400)

# Loop through row counts
parallel --jobs 8 echo "Generating data with {1} rows and average string length of {2}" \; python3 data_generator.py -n {1} -l {2} ::: "${row_counts[@]}" ::: "${string_lengths[@]}"

echo "Data generation complete."
