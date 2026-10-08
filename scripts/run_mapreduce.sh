#!/usr/bin/env bash
set -euo pipefail
export LANG=C.UTF-8 LC_ALL=C.UTF-8
cd "$(dirname "$0")/.."
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME="${HADOOP_HOME:-/home/hadoop/hadoop}"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
BASE="${1:-/user/hadoop/experiment1-qin-$(date +%Y%m%d-%H%M%S)}"
mkdir -p results
printf '%s\n' "$BASE" > results/hdfs-base.txt
for dataset in dedupe sort family edge-dedupe edge-sort edge-family; do
 job=${dataset#edge-}
 hdfs dfs -mkdir -p "$BASE/input/$dataset"
 hdfs dfs -put "data/$dataset/"* "$BASE/input/$dataset/"
 hadoop jar build/experiment1.jar lab.MapReduceJobs "$job" "$BASE/input/$dataset" "$BASE/output/$dataset" 2>&1 | tee "results/$dataset.log"
 hdfs dfs -get "$BASE/output/$dataset/part-r-00000" "results/$dataset.txt"
 hdfs dfs -test -e "$BASE/output/$dataset/_SUCCESS"
done
echo MAPREDUCE_SIX_JOBS_COMPLETE
