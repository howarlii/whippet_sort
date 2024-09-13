# How to compile
```bash
# Make sure you do this in sortproto directory
mkdir build
cd build
cmake -S ..
make
```


# UT Benchmark
现在基于GTest跑准确性测试的时候顺便跑了下benchmark，还没有接入parquet，用的std::string
数据生成方式为：随机前缀长度pre_len[0...max_len]，再随机字符串后缀长度[0...max_len-pre_len]，最后再生成后缀字符串，

benchmark 1, use force cmp (one by one [])
```
==== Test 1
generate data, str_num: 1000000, str_max_len: 50
decode + std::sort time: 1649ms
insert time: 1404ms
output time: 897ms
==== Test 2
generate data, str_num: 10000000, str_max_len: 200
decode + std::sort time: 47653ms
insert time: 15925ms
output time: 16122ms
==== Test 3
generate data, str_num: 50000000, str_max_len: 200
decode + std::sort time: 338991ms
insert time: 96187ms
output time: 103141ms
```


benchmark 2, use uint64 cmp
