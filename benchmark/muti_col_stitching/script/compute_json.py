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

json_path = "./log/1128_064924/data.json"

v_list = ["3", "7", "10", "30"]
method_args = ["1by1", "stitching_all"]
step_names = ["stitching", "sorting", "grouping"]
col_nums = [1, 2, 3, 4, 5]


# read json from file
with open(json_path, "r") as f:
    result = json.load(f)

row_num = "2e7"

for method_arg in method_args:
    for v_r in v_list:
        if v_r not in result[row_num]:
            continue
        time_list = []
        for col_num in col_nums:
            if str(col_num) not in result[row_num][v_r]:
                continue
            tot_time = 0
            # print(result[row_num][v_r])
            for step_name in step_names:
                tot_time += float(result[row_num][v_r][str(col_num)][method_arg]
                                  [step_name])
            time_list.append(tot_time)
        print(f"{method_arg} {v_r} {row_num}: {time_list}")
