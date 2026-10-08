#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export HADOOP_HOME="${HADOOP_HOME:-/home/hadoop/hadoop}"
mkdir -p build/classes
javac -encoding UTF-8 -cp "$("$HADOOP_HOME/bin/hadoop" classpath --glob)" -d build/classes src/lab/*.java
jar --create --file build/experiment1.jar -C build/classes .
echo 'Built build/experiment1.jar'
