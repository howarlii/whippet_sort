import random
import shutil
import time
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor
from subprocess import check_output
from re import findall
import psutil
import threading


benchmark_dir = f"./log/{time.strftime('%m%d_%H%M%S')}/"
os.makedirs(benchmark_dir)

# ============ All Benchmar Args ============
data_seeds = ["0", "19260817", "114514", "1919810"]
row_sizes = ["2e7", "2e8", "2e9"]
value_ranges = [3, 7, 10, 30]
method_args = ["std", "o_by_o", "stitching_all"]
num_cols_list = [1, 2, 3, 4, 5]
# ============ Current Args ============
row_sizes = ["2e7"]
method_args = ["o_by_o", "stitching_all"]
value_ranges = [7]
# =======================================

log_file = f"{benchmark_dir}/benchmark.log"
metrics_log_file = f"{benchmark_dir}/metrics.log"
data_json_file = f"{benchmark_dir}/data.json"


running_benchmark = f"{benchmark_dir}/benchmark_running"


lock = threading.Lock()


def ensure_memory():
    # Ensure enough memory
    total_memory = psutil.virtual_memory().total
    avaliable_memory = psutil.virtual_memory().available
    rate = avaliable_memory / total_memory
    if rate < 0.2:
        print(f"Memory is not enough, {1.0-rate} is using")
        return False
    return True


def run_benchmark(data_path, num_col, method):
    time_to_sleep = 0.5
    std_dev_lmt = 0.15
    while ensure_memory() == False:
        time.sleep(5)
    while True:
        cmd = [
            "valgrind", "--tool=cachegrind", "--branch-sim=yes", "--cache-sim=yes",
            running_benchmark,
            f"--input_file={data_path}",
            f"--num_cols={num_col}",
            f"--num_runs=1",
            f"--warmup=0",
            f"--{method}",
        ]
        print(
            f"Running benchmark with args: {cmd}")

        result = subprocess.run(cmd, capture_output=True, text=True)
        # print(result.stdout)
        with open(log_file, 'a') as f:
            f.write(result.stdout + '\n')
            f.write(result.stderr + '\n')
        # if len(result.stderr) > 0:
        #     print(result.stderr)

        break

    # Extract the JSON-like string from the output
    output_lines = result.stdout.strip().split('\n')
    json_line = "{" + \
        '\n'.join([line for line in output_lines if not line.startswith('#')])+"}"
    # Remove trailing comma before closing brace
    json_line = json_line.replace(',}', '}')
    # Remove trailing comma before newline and closing brace
    json_line = json_line.replace(',\n}', '\n}')

    output = result.stderr
    metrics = {}
    metrics["I refs"] = re.search(r"I refs:\s+([\d,]+)", output).group(1)
    metrics["D refs"] = re.search(r"D refs:\s+([\d,]+)", output).group(1)
    metrics["D1 miss rate"] = re.search(
        r"D1\s+miss rate:\s+([\d.]+%)", output).group(1)
    metrics["LL miss rate"] = re.search(
        r"LL miss rate:\s+([\d.]+%)", output).group(1)
    metrics["Mispred rate"] = re.search(
        r"Mispred rate:\s+([\d.]+%)", output).group(1)

    for k, v in metrics.items():
        v = v.replace(",", "")
        if "%" in v:
            metrics[k] = float(v[:-1])/100
        else:
            metrics[k] = float(v)
    return metrics


def run_benchmark_and_draw(data_name, value_range, num_cols_list=[3, 4, 5], seeds=["0"]):
    metrics_avgs = dict()

    with ThreadPoolExecutor() as executor:
        tasks = dict()
        for method in method_args:
            tasks[method] = dict()
            for num_col in num_cols_list:
                tasks[method][num_col] = dict()
                for seed in seeds:
                    data_path = f"/data/parquet_sorting/int32-ty2-{data_name}-v{value_range}-sed{seed}.parquet"
                    tasks[method][num_col][seed] = executor.submit(
                        run_benchmark, data_path, num_col, method)
                    time.sleep(0.1)

        for num_col in num_cols_list:
            metrics_avgs[num_col] = dict()
            for method in method_args:
                if method not in metrics_avgs[num_col]:
                    metrics_avgs[num_col][method] = dict()
                for seed in seeds:
                    result = tasks[method][num_col][seed].result()
                    if result is None:
                        continue
                    if num_col not in metrics_avgs:
                        metrics_avgs[num_col] = dict()
                    for metric, v in result.items():
                        if metric not in metrics_avgs[num_col]:
                            metrics_avgs[num_col][method][metric] = list()
                            metrics_avgs[num_col][method][metric].append(v)
            # Average the values in the same slot
            for method in metrics_avgs[num_col]:
                for metric in metrics_avgs[num_col][method]:
                    t = metrics_avgs[num_col][method][metric]
                    metrics_avgs[num_col][method][metric] = sum(t) / len(t)

    # print the metrics
    with open(metrics_log_file, 'a') as f:
        f.write(
            f"Data: {data_name}, Value Range: {value_range},  {method_args} \n")
        for method in method_args:
            for num_col in num_cols_list:
                t = metrics_avgs[num_col][method]
                f.write(
                    f'{t["I refs"]}, {t["D refs"]}, {t["D1 miss rate"]},  ')
            f.write("\n")

    return metrics_avgs


def main():
    try:
        shutil.copy2("./build/src/benchmark", running_benchmark)
    except OSError as e:
        print(e)
        print(
            "Warning: Unable to copy the benchmark file. Out date executable might be used.")
        user_input = input("Do you want to continue? (yes/no): ")
        if user_input.lower() != 'yes':
            print("Exiting the program.")
            exit(0)

    current_scr_name = os.path.basename(__file__)
    current_scr_path = os.path.relpath(__file__)
    shutil.copy2(f"./{current_scr_path}",
                 f"{benchmark_dir}/{current_scr_name}")

    results = dict()

    for row_num in row_sizes:
        results[row_num] = dict()
        for value_range in value_ranges:
            results[row_num][value_range] = run_benchmark_and_draw(
                f"{row_num}", value_range, num_cols_list, data_seeds)
            with open(data_json_file, 'w') as f:
                f.write(json.dumps(results) + '\n')

    print("All benchmarks are done.")
    print("========================")
    print(json.dumps(results))
    print("\n")


main()
# print(get_a_bind_id_func())
