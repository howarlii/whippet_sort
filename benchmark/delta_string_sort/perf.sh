#!/bin/bash

flamegraph_dir="/workspace/FlameGraph/"

# Check if at least one argument (the executable path) is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <executable_path> [args...]"
    exit 1
fi

# Capture the executable path and the rest of the arguments
executable="$1"
shift # Shift the arguments so $@ now contains only the executable's arguments

# Run the executable with perf record
echo "Running $executable with perf record..."
perf record -a -s -g -F 10000 "$executable" "$@"

perf script >out.perf

$flamegraph_dir/stackcollapse-perf.pl out.perf >out.folded
$flamegraph_dir/flamegraph.pl out.folded >kernel.svg
