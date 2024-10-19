import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

import subprocess
import json
import itertools

data_name = "2e6-200"
data_path = f"./data/input-{data_name}.parquet"


def run_benchmark(lazy_dep_lmt, lazy_key_burst_lmt):
    cmd = [
        "./build/src/benchmark",
        f"--input_file={os.path.abspath(data_path)}",
        f"--trie_lazy_dep_lmt={lazy_dep_lmt}",
        f"--trie_lazy_key_burst_lmt={lazy_key_burst_lmt}",
        "--sort_col_idx=2",
        "--trie"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)

    # Extract the JSON-like string from the output
    output_lines = result.stdout.strip().split('\n')
    json_line = "{" + \
        '\n'.join([line for line in output_lines if not line.startswith('#')])+"}"
    # Remove trailing comma before closing brace
    json_line = json_line.replace(',}', '}')
    # Remove trailing comma before newline and closing brace
    json_line = json_line.replace(',\n}', '\n}')

    if json_line:
        try:
            return json.loads(json_line)
        except json.JSONDecodeError:
            print(f"Error decoding JSON from: {json_line}")
            return None
    else:
        print("No JSON data found in the output")
        return None


# Define the range of values to test
lazy_dep_lmt_range = [3, 4, 5, 6]
lazy_key_burst_lmt_range = [512, 1024, 2048, 4096, 8192, 16384]

results = []

for lazy_dep_lmt, lazy_key_burst_lmt in itertools.product(lazy_dep_lmt_range, lazy_key_burst_lmt_range):
    result = run_benchmark(lazy_dep_lmt, lazy_key_burst_lmt)
    if result:
        result = result['Trie']
        result['lazy_dep_lmt'] = lazy_dep_lmt
        result['lazy_key_burst_lmt'] = lazy_key_burst_lmt
        results.append(result)

# Now 'results' contains all the benchmark results
print(f"Collected {len(results)} benchmark results")

# Convert results to a DataFrame for easier analysis and visualization
df = pd.DataFrame(results)


def draw_plot(path_pref, step_name):
    plt.figure(figsize=(12, 6))
    for dep_lmt in df['lazy_dep_lmt'].unique():
        subset = df[df['lazy_dep_lmt'] == dep_lmt]
        plt.plot(subset['lazy_key_burst_lmt'], subset[step_name],
                 marker='o', linestyle='-', label=f'lazy_dep_lmt = {dep_lmt}')

    plt.xlabel('Lazy Key Burst Limit (lazy_key_burst_lmt)')
    plt.ylabel('Execution Time (ms)')
    plt.title('Execution Time vs Lazy Key Burst Limit')
    plt.legend()
    plt.grid(True)
    # Using log scale for x-axis as key_burst_lmt values are powers of 2
    plt.xscale('log')

    # Save the figure to a file
    plt.savefig(f'{path_pref}_{step_name}_vs_key_burst_lmt.png',
                dpi=300, bbox_inches='tight')
    plt.close()

    print(f"Figure saved as '{path_pref}_{step_name}_vs_key_burst_lmt.png'")


def draw_plot_tot(path_pref, step_name):
    plt.figure(figsize=(12, 6))
    for dep_lmt in df['lazy_dep_lmt'].unique():
        subset = df[df['lazy_dep_lmt'] == dep_lmt]
        plt.plot(subset['lazy_key_burst_lmt'], subset['read+sort'] + subset['generate result'],
                 marker='o', linestyle='-', label=f'lazy_dep_lmt = {dep_lmt}')

    plt.xlabel('Lazy Key Burst Limit (lazy_key_burst_lmt)')
    plt.ylabel('Execution Time (ms)')
    plt.title('Execution Time vs Lazy Key Burst Limit')
    plt.legend()
    plt.grid(True)
    # Using log scale for x-axis as key_burst_lmt values are powers of 2
    plt.xscale('log')

    # Save the figure to a file
    plt.savefig(f'{path_pref}_{step_name}_vs_key_burst_lmt.png',
                dpi=300, bbox_inches='tight')
    plt.close()

    print(f"Figure saved as '{path_pref}_{step_name}_vs_key_burst_lmt.png'")


draw_plot(f'figs/{data_name}', 'read+sort')
draw_plot(f'figs/{data_name}', 'generate result')
draw_plot_tot(f'figs/{data_name}', 'total')
