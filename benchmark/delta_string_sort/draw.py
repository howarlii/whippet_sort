import shutil
import time
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import json
import itertools
import os
from concurrent.futures import ProcessPoolExecutor


benchmark_dir = f"./log/{time.strftime('%m%d_%H%M%S')}/"
os.makedirs(benchmark_dir)

# ============ benchmar args ============
burst_dep = 4
burst_size_lmt = 4096

row_sizes = ["2e6"]
str_lengths = [10, 20, 100, 800, 1600]
col_idxs = [0, 1, 2]
# =======================================

log_file = f"{benchmark_dir}/draw.log"
data_json_file = f"{benchmark_dir}/data.json"


running_benchmark = f"{benchmark_dir}/benchmark_running"
core_bind_id = "191"
# core_bind_id = "95"


def run_benchmark(data_path, sort_col_idx, lazy_dep_lmt, lazy_key_burst_lmt):
    cmd = [
        "taskset",
        "-c",
        core_bind_id,
        running_benchmark,
        f"--input_file={data_path}",
        f"--trie_lazy_dep_lmt={lazy_dep_lmt}",
        f"--trie_lazy_key_burst_lmt={lazy_key_burst_lmt}",
        f"--sort_col_idx={sort_col_idx}",
        # "--low_arrow",
        # "--trie",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    with open(log_file, 'a') as f:
        f.write(result.stdout + '\n')
    print(result.stderr)

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


def run_benchmark_and_draw(data_name, data_path="", col_idxs=[2], burst_dep=4, burst_size_lmt=4096):
    if data_path == "":
        data_path = f"./data/input-ty2-{data_name}.parquet"

    fig, axs = plt.subplots(1, len(col_idxs), figsize=(15, 5), sharey=True)
    if len(col_idxs) == 1:
        axs = [axs]  # Convert single axis to list for consistency

    step_time_avgs = dict()
    # with ProcessPoolExecutor() as executor:
    #     tasks = dict()
    #     for col_idx in col_idxs:
    #         tasks[col_idx] = executor.submit(
    #             run_benchmark, data_path, col_idx, burst_dep, burst_size_lmt)
    #     for col_idx in col_idxs:
    #         step_time_avgs[col_idx] = tasks[col_idx].result()
    for col_idx in col_idxs:
        step_time_avgs[col_idx] = run_benchmark(
            data_path, col_idx, burst_dep, burst_size_lmt)

    for i, col_idx in enumerate(col_idxs):
        title_str = f"{data_name}-col{col_idx}-bdep{burst_dep}-bsize{burst_size_lmt}"
        step_time_avg = step_time_avgs[col_idx]

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
                            f'{step_name}\n{step_time}ms', ha='center', va='center', color='black', fontsize=8)

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


try:
    shutil.copy2("./build/src/benchmark", running_benchmark)
except OSError as e:
    print(e)
    print("Warning: Unable to copy the benchmark file. Out date executable might be used.")
    user_input = input("Do you want to continue? (yes/no): ")
    if user_input.lower() != 'yes':
        print("Exiting the program.")
        exit(0)

shutil.copy2("./draw.py", f"{benchmark_dir}/draw.py")

results = dict(dict())


def func(length, size, col_idxs):
    results[length][size] = run_benchmark_and_draw(f"{size}-{length}", "",
                                                   col_idxs, burst_dep, burst_size_lmt)


for str_len in str_lengths:
    results[str_len] = dict()

# for str_len in str_lengths:
#     func(str_len, "2e7", [1])

for str_len in str_lengths:
    func(str_len, "2e6", col_idxs)

# for len in str_lengths:
#     func(len, "2e6", [1])


# os.remove(running_benchmark)

print("All benchmarks are done.")
print("========================")
print(json.dumps(results))
print("\n")

with open(data_json_file, 'w') as f:
    f.write(json.dumps(results) + '\n')
