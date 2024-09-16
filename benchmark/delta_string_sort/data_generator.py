import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

n_rows_str = "2e5"

# Number of rows
n_rows = int(float(n_rows_str))

# Generate random string lengths centered around 20, between 15 and 25
string_lengths = np.random.randint(30, 50, size=n_rows)

# Generate a 1D array of random 'a', 'b', 'c' characters, and flatten the array
total_chars = string_lengths.sum()
char_array = np.random.choice(list('abc'), size=total_chars)

# Create an array of indices to split the char_array into strings based on the random lengths
split_indices = np.cumsum(string_lengths[:-1])

# Split the array of characters into the individual strings
string_col = np.split(char_array, split_indices)
string_col = [''.join(s) for s in string_col]

# Use NumPy to efficiently generate other columns
int_col = np.arange(n_rows, dtype=np.int32)  # Vectorized int generation

# Create a pandas DataFrame
df = pd.DataFrame({
    'string_col': string_col,
    'string_col2': string_col,
    'int_col': int_col,
})

# Define the schema with pyarrow
schema = pa.schema([
    ('string_col', pa.string()),
    ('string_col2', pa.string()),
    ('int_col', pa.int32()),
])

# Convert the pandas DataFrame to a pyarrow Table
table = pa.Table.from_pandas(df, schema=schema)

# Write the table to a Parquet file
pq.write_table(
    table,
    f'data/input-{n_rows_str}-40.parquet',
    compression='SNAPPY',  # Optional: Compression
    use_dictionary=False,   # Optional: Use dictionary encoding
    data_page_version='2.0',  # Parquet v2
    column_encoding={'string_col2': 'DELTA_BYTE_ARRAY'}  # Optional: Encoding
)

print("Parquet file 'example_random_variable_length_strings.parquet' has been created.")
