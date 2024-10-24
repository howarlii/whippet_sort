import pyarrow.parquet as pq


def check_column_encodings(parquet_file_path):
    # Open the Parquet file
    parquet_file = pq.ParquetFile(parquet_file_path)

    # Get the file metadata
    metadata = parquet_file.metadata

    # Loop over each row group in the file
    for row_group_idx in range(metadata.num_row_groups):
        print(f"Row Group {row_group_idx}:")

        # Get the row group metadata
        row_group = metadata.row_group(row_group_idx)

        # Loop over each column in the row group
        for column_idx in range(row_group.num_columns):
            # Get the column metadata
            column = row_group.column(column_idx)
            column_name = column.path_in_schema
            encoding = column.encodings

            # Print the column name and encoding methods used
            print(
                f"  Column '{column_name}' uses the following encodings: {encoding}")

    print("Encoding check complete.")


# Example usage
# Replace with the path to your Parquet file
parquet_file_path = './data/input-2e5-100.parquet'
check_column_encodings(parquet_file_path)
