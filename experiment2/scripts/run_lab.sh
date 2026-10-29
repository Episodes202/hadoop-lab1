#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export LANG=C.UTF-8 LC_ALL=C.UTF-8
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/home/hadoop/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"
STAMP="$(date +%Y%m%d-%H%M%S)-$$"
BASE="/user/$(id -un)/experiment2/$STAMP"
RUN="results/$STAMP"
mkdir -p "$RUN"
printf '%s\n' "$BASE" > "$RUN/hdfs-base.txt"
printf '%s\n' "$RUN" > results/latest-run.txt
bash build.sh 2>&1 | tee "$RUN/build.log"
{
  date --iso-8601=seconds
  uname -a
  cat /etc/os-release
  java -version
  hadoop version
  jps -l
  free -m
  yarn node -list
} > "$RUN/environment.log" 2>&1
hdfs dfs -mkdir -p "$BASE/input"
hdfs dfs -put data/corpus "$BASE/input/corpus"
hdfs dfs -put data/edge "$BASE/input/edge"
hdfs dfs -put data/stopwords.txt "$BASE/stopwords.txt"
hdfs dfs -ls -R "$BASE/input" > "$RUN/hdfs-input-listing.txt"

run_job() {
  local name="$1" input="$2" reducers="$3" combiner="$4"
  timeout 600 hadoop jar build/experiment2.jar lab.InvertedIndex \
    "-Dlab.index.combiner.enabled=$combiner" \
    -Dmapreduce.map.speculative=false -Dmapreduce.reduce.speculative=false \
    "$BASE/input/$input" "$BASE/output-$name" "$BASE/stopwords.txt" "$reducers" \
    2>&1 | tee "$RUN/$name.log"
  hdfs dfs -test -e "$BASE/output-$name/_SUCCESS"
  hdfs dfs -get "$BASE/output-$name" "$RUN/$name"
  python3 tests/verify_index.py "data/$input" data/stopwords.txt "$RUN/$name" \
    --summary "$RUN/$name-verification.json" > "$RUN/$name-verification.log"
  cat "$RUN/$name-verification.log"
}

run_job corpus corpus 1 true
run_job edge-combiner edge 2 true
run_job edge-no-combiner edge 2 false
printf 'Completed. Local results: %s\nHDFS base: %s\n' "$RUN" "$BASE"
