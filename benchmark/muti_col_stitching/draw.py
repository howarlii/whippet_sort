import random
import shutil
import time
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import json
import itertools
import os
from concurrent.futures import ThreadPoolExecutor
from subprocess import check_output
from re import findall
import psutil
import threading


benchmark_dir = f"./log/{time.strftime('%m%d_%H%M%S')}/"
os.makedirs(benchmark_dir)

# ============ benchmar args ============
row_sizes = ["2e6", "2e7", "2e8", "2e9"]
method_args = ["std", "o_by_o", "stitching_all"]
num_cols_list = [3, 4, 5]
# =======================================

log_file = f"{benchmark_dir}/benchmark.log"
draw_log_file = f"{benchmark_dir}/draw.log"
data_json_file = f"{benchmark_dir}/data.json"


running_benchmark = f"{benchmark_dir}/benchmark_running"


lock = threading.Lock()


def get_a_bind_id_func():
    """
    Finds an idle CPU core with usage below a specified threshold.

    Args:
        threshold (int): Usage threshold below which a CPU core is considered idle.

    Returns:
        int: Index of an idle CPU core, or -1 if none are idle.
    """
    lock.acquire()
    threshold = 10
    while True:
        cpu_percentages = psutil.cpu_percent(percpu=True, interval=0.1)
        idle_cores = []
        for i in range(0, len(cpu_percentages), 2):
            if cpu_percentages[i] + cpu_percentages[i+1] < threshold:
                idle_cores.append(i)
        if len(idle_cores) > 0:
            lock.release()
            return str(random.choice(idle_cores))
        # Wait a bit before checking again to avoid tight looping
        time.sleep(1)
        threshold += 1


def run_benchmark(data_path, num_col, method):
    time_to_sleep = 0.5
    std_dev_lmt = 0.15
    while True:
        bind_core_id = get_a_bind_id_func()
        cmd = [
            "taskset",
            "-c",
            bind_core_id,
            running_benchmark,
            f"--input_file={data_path}",
            f"--std_dev_lmt={std_dev_lmt}",
            f"--num_cols={num_col}",
            f"--num_runs=5",
            f"--warmup=1",
            f"--{method}",
        ]
        print(
            f"Running benchmark with args: {cmd}")

        result = subprocess.run(cmd, capture_output=True, text=True)
        # print(result.stdout)
        with open(log_file, 'a') as f:
            f.write(result.stdout + '\n')
        with open(log_file, 'a') as f:
            f.write(result.stderr + '\n')
        if len(result.stderr) > 0:
            print(result.stderr)

        if result.returncode != 0:
            if std_dev_lmt > 0.2:
                return None
            time.sleep(time_to_sleep)
            std_dev_lmt += 0.01
            continue
        break

    with open(draw_log_file, 'a') as f:
        f.write(result.stdout + '\n')
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


def run_benchmark_and_draw(data_name, num_cols_list=[3, 4, 5], seeds=["0"]):
    step_time_avgs = dict()

    with ThreadPoolExecutor(max_workers=16) as executor:
        tasks = dict()
        for method in method_args:
            tasks[method] = dict()
            for num_col in num_cols_list:
                tasks[method][num_col] = dict()
                for seed in seeds:
                    data_path = f"/data/parquet_sorting/int32-ty1-{data_name}-sed{seed}.parquet"
                    tasks[method][num_col][seed] = executor.submit(
                        run_benchmark, data_path, num_col, method)
                    time.sleep(0.5)

        for num_col in num_cols_list:
            for method0 in method_args:
                for seed in seeds:
                    result = tasks[method0][num_col][seed].result()
                    if result is None:
                        continue
                    if num_col not in step_time_avgs:
                        step_time_avgs[num_col] = dict()
                    for method, steps in result.items():
                        if method not in step_time_avgs[num_col]:
                            step_time_avgs[num_col][method] = dict()
                        for step, time_ms in steps.items():
                            if step not in step_time_avgs[num_col][method]:
                                step_time_avgs[num_col][method][step] = []
                            step_time_avgs[num_col][method][step].append(
                                time_ms)
            # Average the values in the same slot
            for method in step_time_avgs[num_col]:
                for step in step_time_avgs[num_col][method]:
                    t = step_time_avgs[num_col][method][step]
                    step_time_avgs[num_col][method][step] = sum(t) / len(t)

    # Draw figures
    fig, axs = plt.subplots(1, len(num_cols_list),
                            figsize=(15, 5), sharey=True)
    if len(num_cols_list) == 1:
        axs = [axs]  # Convert single axis to list for consistency
    for i, num_col in enumerate(num_cols_list):
        title_str = f"{data_name}-col{num_col}"
        step_time_avg = step_time_avgs[num_col]

        # 提取方法名称
        methods = list(step_time_avg.keys())

        # 获取每个方法的步骤和对应的时间
        # 我们要确保每个方法的步骤数量不同是可以处理的
        step_times = [list(steps.values()) for steps in step_time_avg.values()]
        step_names = [list(steps.keys()) for steps in step_time_avg.values()]

        # 设置柱子的位置
        bar_width = 0.8
        index = np.arange(len(methods))

        # 设置颜色，每个步骤将使用不同颜色
        colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#c2c2f0']

        # 绘制每个方法的柱状图
        for j, (method, steps) in enumerate(step_time_avg.items()):
            bottom = 0  # 每个柱子的初始底部位置为0
            total_time = 0  # 初始化每个方法的总耗时
            for k, (step_name, step_time) in enumerate(steps.items()):
                if step_time == 0:
                    continue
                # 绘制每个步骤的柱子部分
                axs[i].bar(index[j], step_time, bar_width, bottom=bottom,
                           label=step_name if j == 0 else "", color=colors[k % len(colors)])

                # 在图中添加步骤名称和对应的时间
                axs[i].text(index[j], bottom + step_time / 2,
                            f'{step_name}\n{step_time:.1f}ms', ha='center', va='center', color='black', fontsize=8)

                # 更新底部位置以便堆叠
                bottom += step_time
                total_time += step_time  # 计算总耗时

            # 在每个柱子的顶端添加总耗时
            axs[i].text(index[j], bottom + 10, f'Total\n{int(total_time)}ms',
                        ha='center', va='bottom', color='black', fontsize=10)

        # 添加标签和标题
        axs[i].set_xlabel('Methods')
        axs[i].set_ylabel('Time (ms)')
        axs[i].set_title(f'{title_str}')
        axs[i].set_xticks(index)
        axs[i].set_xticklabels(methods)

        # 图例只显示一次（使用去重方式）
        handles, labels = axs[i].get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        # axs[i].legend(by_label.values(), by_label.keys())

        # 减少柱状图之间的间隙
        axs[i].set_xticklabels(methods, rotation=45, ha="right")
        axs[i].tick_params(axis='x', which='both',
                           bottom=False, top=False, labelbottom=True)

    # 显示图形
    plt.tight_layout()
    plt.savefig(f'{benchmark_dir}/output-{data_name}-combined.png')
    print(f'image {benchmark_dir}/output-{data_name}-combined.png saved')
    # plt.show()
    return step_time_avgs


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

    shutil.copy2("./draw.py", f"{benchmark_dir}/draw.py")

    results = dict()

    for row_num in row_sizes:
        results[row_num] = run_benchmark_and_draw(f"{row_num}", num_cols_list, [
                                                  "0", "19260817", "114514", "1919810"])
        with open(data_json_file, 'w') as f:
            f.write(json.dumps(results) + '\n')

    print("All benchmarks are done.")
    print("========================")
    print(json.dumps(results))
    print("\n")


main()
# print(get_a_bind_id_func())
