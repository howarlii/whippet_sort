import shutil
import time
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import json
import itertools
import os
from concurrent.futures import ProcessPoolExecutor

json_path = "log/1108_050119/data.json"

with open(json_path, 'r') as f:
    data = json.load(f)


str_lengths = [10, 20, 100, 800]

for method in ["read", "sort"]:
    for str_len in str_lengths:
        print(data[str(str_len)]['2e7']["1"]["Arrow"][method], end=", ")
    print()

for method in ["read", "build", "pre-sort", "print-trie"]:
    for str_len in str_lengths:
        print(data[str(str_len)]['2e7']["1"]["Trie"][method], end=", ")
    print()

print()
print()
print()

cols = ["0", "1", "2"]
for method in ["read", "sort"]:
    for col in cols:
        print(data["1600"]['2e7'][col]["Arrow"][method], end=", ")
    print()

for method in ["read", "build", "pre-sort", "print-trie"]:
    for col in cols:
        print(data["1600"]['2e7'][col]["Trie"][method], end=", ")
    print()
