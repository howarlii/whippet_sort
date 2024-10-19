import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import argparse
import os

# Set up argument parser
parser = argparse.ArgumentParser(
    description='Generate Parquet file with random data')
parser.add_argument('--n_rows', '-n', type=str, default="2e6",
                    help='Number of rows (can be in scientific notation)')
parser.add_argument('--str_len_avg', '-l', type=int,
                    default=200, help='Average length of strings')
parser.add_argument('--test', type=bool,
                    default=False, help='Test mode')

# Parse arguments
args = parser.parse_args()

n_rows_str = args.n_rows
str_len_avg = args.str_len_avg


# Number of rows
n_rows = int(float(n_rows_str))


def gen_rnd_str_len(n_rows, str_len_avg):
    # Generate random string lengths centered around
    string_lengths = np.random.randint(
        str_len_avg*0.8, str_len_avg*1.2, size=n_rows)

    # Generate a 1D array of random 'a', 'b', 'c' characters, and flatten the array
    total_chars = string_lengths.sum()
    char_array = np.random.choice(list('abc'), size=total_chars)

    # Create an array of indices to split the char_array into strings based on the random lengths
    split_indices = np.cumsum(string_lengths[:-1])

    # Split the array of characters into the individual strings
    string_col = np.split(char_array, split_indices)
    string_col = [''.join(s) for s in string_col]
    return string_col


def gen_block_str_len(n_rows, str_len_avg):
    block_len = 10
    block_num = 100
    # Generate random string lengths centered around
    string_lengths = np.random.randint(
        str_len_avg/block_len*0.8, str_len_avg/block_len*1.2, size=n_rows)

    blocks = []
    for i in range(block_num):
        blocks.append(''.join(np.random.choice(
            list('abcdefghijklmnopqrstuvwxyz'), size=block_len)))

    # Generate a 1D array of random 'a', 'b', 'c' characters, and flatten the array
    total_chars = string_lengths.sum()
    char_array = np.random.choice(blocks, size=total_chars)

    # Create an array of indices to split the char_array into strings based on the random lengths
    split_indices = np.cumsum(string_lengths[:-1])

    # Split the array of characters into the individual strings
    string_col = np.split(char_array, split_indices)
    string_col = [''.join(s) for s in string_col]
    return string_col


# Use NumPy to efficiently generate other columns
int_col = np.arange(n_rows, dtype=np.int32)  # Vectorized int generation

# Create a pandas DataFrame
df = pd.DataFrame({
    'string_col0': gen_rnd_str_len(n_rows, str_len_avg),
    'string_col1': gen_rnd_str_len(n_rows, str_len_avg),
    'string_col2': gen_block_str_len(n_rows, str_len_avg),
    'int_col': int_col,
})

# Define the schema with pyarrow
schema = pa.schema([
    ('string_col0', pa.string()),
    ('string_col1', pa.string()),
    ('string_col2', pa.string()),
    ('int_col', pa.int32()),
])

# Convert the pandas DataFrame to a pyarrow Table
table = pa.Table.from_pandas(df, schema=schema)

if args.test:
    print(table.to_pandas())
    exit()

# Write the table to a Parquet file
file_name = f'data/input-{n_rows_str}-{str_len_avg}.parquet'
pq.write_table(
    table,
    file_name,
    compression='SNAPPY',  # Optional: Compression
    use_dictionary=False,   # Optional: Use dictionary encoding
    data_page_version='2.0',  # Parquet v2
    column_encoding={'string_col1': 'DELTA_BYTE_ARRAY',
                     'string_col2': 'DELTA_BYTE_ARRAY'}  # Optional: Encoding
)

print(
    f"Parquet file '{file_name}' has been created. Size: {os.path.getsize(file_name)/1024/1024:.2f} MB")
