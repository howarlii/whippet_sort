import matplotlib.pyplot as plt
import numpy as np
import subprocess
import json
import itertools
import os


def run_benchmark(data_path, lazy_dep_lmt, lazy_key_burst_lmt):
    cmd = [
        "./build/src/benchmark",
        f"--input_file={data_path}",
        f"--trie_lazy_dep_lmt={lazy_dep_lmt}",
        f"--trie_lazy_key_burst_lmt={lazy_key_burst_lmt}",
        "--sort_col_idx=2",
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


def run_benchmark_and_draw(data_name, data_path="", lazy_dep_lmt=4, lazy_key_burst_lmt=4096):
    if data_path == "":
        data_path = f"./data/input-{data_name}.parquet"

    title_str = f"{data_name}-{lazy_dep_lmt}-{lazy_key_burst_lmt}"
    step_time_avg = run_benchmark(data_path, lazy_dep_lmt, lazy_key_burst_lmt)

    # 提取方法名称
    methods = list(step_time_avg.keys())

    # 获取每个方法的步骤和对应的时间
    # 我们要确保每个方法的步骤数量不同是可以处理的
    step_times = [list(steps.values()) for steps in step_time_avg.values()]
    step_names = [list(steps.keys()) for steps in step_time_avg.values()]

    # 设置柱子的位置
    bar_width = 0.5
    index = np.arange(len(methods))

    # 创建图形
    fig, ax = plt.subplots()

    # 设置颜色，每个步骤将使用不同颜色
    colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#c2c2f0']

    # 绘制每个方法的柱状图
    for i, (method, steps) in enumerate(step_time_avg.items()):
        bottom = 0  # 每个柱子的初始底部位置为0
        total_time = 0  # 初始化每个方法的总耗时
        for j, (step_name, step_time) in enumerate(steps.items()):
            if step_time == 0:
                continue
            # 绘制每个步骤的柱子部分
            ax.bar(index[i], step_time, bar_width, bottom=bottom,
                   label=step_name if i == 0 else "", color=colors[j % len(colors)])

            # 在图中添加步骤名称和对应的时间
            ax.text(index[i], bottom + step_time / 2,
                    f'{step_name}\n{step_time}ms', ha='center', va='center', color='black', fontsize=10)

            # 更新底部位置以便堆叠
            bottom += step_time
            total_time += step_time  # 计算总耗时

        # 在每个柱子的顶端添加总耗时
        ax.text(index[i], bottom + 100, f'Total: {int(total_time)}ms',
                ha='center', va='bottom', color='black', fontsize=12)

    # 添加标签和标题
    ax.set_xlabel('Methods')
    ax.set_ylabel('Time (ms)')
    ax.set_title(f'{title_str}')
    ax.set_xticks(index)
    ax.set_xticklabels(methods)

    # 图例只显示一次（使用去重方式）
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys())

    # 显示图形
    plt.tight_layout()
    plt.savefig(f'figs/output-{title_str}.png')
    print(f'image figs/output-{title_str}.png saved')
    # plt.show()


lazy_dep_lmt = 1000000000
lazy_key_burst_lmt = 4096

run_benchmark_and_draw("2e5-100", "", lazy_dep_lmt, lazy_key_burst_lmt)
run_benchmark_and_draw("2e5-200", "", lazy_dep_lmt, lazy_key_burst_lmt)
run_benchmark_and_draw("2e5-400", "", lazy_dep_lmt, lazy_key_burst_lmt)

run_benchmark_and_draw("2e6-100", "", lazy_dep_lmt, lazy_key_burst_lmt)
run_benchmark_and_draw("2e6-200", "", lazy_dep_lmt, lazy_key_burst_lmt)
run_benchmark_and_draw("2e6-400", "", lazy_dep_lmt, lazy_key_burst_lmt)

run_benchmark_and_draw("2e7-100", "", lazy_dep_lmt, lazy_key_burst_lmt)
run_benchmark_and_draw("2e7-200", "", lazy_dep_lmt, lazy_key_burst_lmt)
# run_benchmark_and_draw("2e7-400", "", lazy_dep_lmt, lazy_key_burst_lmt)
