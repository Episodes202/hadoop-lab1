#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
export LANG=C.UTF-8 LC_ALL=C.UTF-8
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/home/hadoop/hadoop}"
mkdir -p build/classes
"$JAVA_HOME/bin/javac" --release 17 -encoding UTF-8 -cp "$("$HADOOP_HOME/bin/hadoop" classpath --glob)" -d build/classes src/lab/InvertedIndex.java
"$JAVA_HOME/bin/jar" --create --file build/experiment2.jar -C build/classes .
printf 'Built %s/build/experiment2.jar\n' "$PWD"
