import matplotlib.pyplot as plt
import numpy as np

# 每个方法、步骤的平均耗时（单位：毫秒）
step_time_avg = dict()

# ====================================================================== result for 2e7
title_str = "2e7 x 40"
step_time_avg["Arrow"] = {
"read": 3861.2,
"sort": 20360.6,
"generate result": 12187.2,
}
# Arrow sorting - Median: 35934ms, Average: 36409ms

step_time_avg["Trie-Arrow"] = {
"read": 3233.4,
"sort": 21688,
"generate result": 5529.2,
}
# Whippet sorting (Trie-Arrow) - Median: 30494ms, Average: 30450.6ms

step_time_avg["Trie"] = {
"":0,
"read+sort": 32484,
"generate result": 34755.2,
}
# Whippet sorting (Trie) - Median: 67492ms, Average: 67239.2ms
# Whippet sorting (Trie) - Median: 75652ms, Average: 76179.6ms
# ====================================================================== result for 2e6
title_str = "2e6 x 40"
step_time_avg["Arrow"] = {
"read": 390.4,
"sort": 1300.8,
"generate result": 996.2,
}
# Arrow sorting - Median: 2661ms, Average: 2687.4ms

step_time_avg["Trie-Arrow"] = {
"read": 307.8,
"sort": 1054.4,
"generate result": 352.8,
}
# Whippet sorting (Trie-Arrow) - Median: 1712ms, Average: 1715ms

step_time_avg["Trie"] = {
"":0,
"read+sort": 2253.2,
"generate result": 3078.2,
}
# Whippet sorting (Trie) - Median: 5284ms, Average: 5331.4ms
# ======================================================================

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
        # 绘制每个步骤的柱子部分
        ax.bar(index[i], step_time, bar_width, bottom=bottom, label=step_name if i == 0 else "", color=colors[j % len(colors)])

        # 在图中添加步骤名称和对应的时间
        ax.text(index[i], bottom + step_time / 2, f'{step_name}\n{step_time}ms', ha='center', va='center', color='black', fontsize=10)

        # 更新底部位置以便堆叠
        bottom += step_time
        total_time += step_time  # 计算总耗时

    # 在每个柱子的顶端添加总耗时
    ax.text(index[i], bottom + 100, f'Total: {int(total_time)}ms', ha='center', va='bottom', color='black', fontsize=12)


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
plt.savefig(f'output-{title_str}.png')
# plt.show()
