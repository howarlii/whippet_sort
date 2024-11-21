#pragma once
#include <algorithm>
#include <cmath>
#include <functional>
#include <iostream>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "utils.h"

namespace whippet_sort::utils {

/**
 * @class Benchmarker
 * @brief A class to benchmark the performance of code segments.
 *
 * This class provides functionality to measure the running time of code
 * segments in microseconds. It can perform multiple runs and calculate the
 * standard deviation of the running times.
 *
 * @typedef BenchmarkStep
 * @brief A type alias for a function that returns the running time in
 * milliseconds. If return 0, benchmarker will use the running time of the whole
 * function.
 *
 * @param name The name of the benchmark.
 */
class BenchmarkHelper {
public:
  // return running time in milliseconds. If return 0, benchmarker will use
  // the running time of the whole function.
  using BenchmarkStep = std::function<double()>;

  BenchmarkHelper(const std::string &name) : name_(name) {}

  void add_step(const std::string &name, BenchmarkStep step) {
    steps_.push_back({name, std::move(step)});
  }

  void warmup(int num = 1) {
    for (int round = 0; round < num; ++round) {
      for (int step_i = 0; step_i < steps_.size(); ++step_i) {
        try {
          std::get<1>(steps_[step_i])();
        } catch (const std::exception &e) {
          std::cerr << "Error in step " << step_i << ": " << e.what()
                    << std::endl;
        }
      }
    }
  }

  void run(int num_runs) {
    durations_ms_.resize(steps_.size());
    std::fill(durations_ms_.begin(), durations_ms_.end(),
              std::vector<double>(num_runs));
    tot_durations_ms_.resize(num_runs);

    for (int round = 0; round < num_runs; ++round) {
      double tot_duration_ms = 0;
      for (size_t step_i = 0; step_i < steps_.size(); ++step_i) {
        Timer timer;
        double time_cost_ms = 0;
        timer.start();
        try {
          time_cost_ms = std::get<1>(steps_[step_i])();
          timer.stop();
          if (time_cost_ms == 0) {
            time_cost_ms = timer.get_ms();
          }
        } catch (const std::exception &e) {
          time_cost_ms = 0;
          std::cerr << "Error in step " << step_i << ": " << e.what()
                    << std::endl;
        }

        durations_ms_[step_i][round] = time_cost_ms;
        tot_duration_ms += time_cost_ms;
      }
      tot_durations_ms_[round] = tot_duration_ms;
    }

    auto [avg, std] = compute_statics(tot_durations_ms_);
    average_ms_ = avg;
    std_dev_ = std;

    // Calculate median
    std::sort(tot_durations_ms_.begin(), tot_durations_ms_.end());
    median_ms_ = tot_durations_ms_[tot_durations_ms_.size() / 2];
  }

  std::string get_detail_json() {
    std::string json = fmt::format("\"{}\":  {}\n", name_, "{");
    for (size_t step_i = 0; step_i < steps_.size(); ++step_i) {
      auto [avg_ms, stdev] = compute_statics(durations_ms_[step_i]);

      // Calculate median
      std::sort(durations_ms_[step_i].begin(), durations_ms_[step_i].end());
      double median_ms =
          durations_ms_[step_i][durations_ms_[step_i].size() / 2];
      if (durations_ms_[step_i].front() < 1) {
        avg_ms = 0;
        median_ms = 0;
      }
      json += fmt::format("\"{}\": {:.1f}{}\n", std::get<0>(steps_[step_i]),
                          median_ms, step_i == steps_.size() - 1 ? "" : ",");
    }
    json += "}\n";
    return json;
  }

  std::tuple<double, double, double> get_tot_statics() {
    return {median_ms_, average_ms_, std_dev_};
  }

private:
  std::pair<double, double> compute_statics(const std::vector<double> &a) {
    double sum = std::accumulate(a.begin(), a.end(), 0.0);
    double mean = sum / a.size();
    double sq_sum = std::inner_product(a.begin(), a.end(), a.begin(), 0.0);
    double stdev = std::sqrt(sq_sum / a.size() - mean * mean);
    return {mean, stdev};
  }

  std::string name_;
  std::vector<std::pair<std::string, BenchmarkStep>> steps_;

  std::vector<std::vector<double>> durations_ms_;
  std::vector<double> tot_durations_ms_;

  double median_ms_, average_ms_, std_dev_;
};

} // namespace whippet_sort::utils