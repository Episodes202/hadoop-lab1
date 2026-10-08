#!/usr/bin/env bash
set -euo pipefail
export LANG=C.UTF-8 LC_ALL=C.UTF-8
export HADOOP_HOME="${HADOOP_HOME:-/home/hadoop/hadoop}"
HDFS="$HADOOP_HOME/bin/hdfs"
cmd="${1:?Operation required}"; shift
case "$cmd" in
 upload)
  src=$1; dst=$2; mode=${3:-}
  if [ -z "$mode" ]; then read -r -p 'append or overwrite: ' mode; fi
  case "$mode" in append|overwrite) ;; *) echo 'Invalid mode' >&2; exit 2;; esac
  "$HDFS" dfs -mkdir -p "$(dirname "$dst")"
  if "$HDFS" dfs -test -d "$dst"; then echo 'Destination is a directory' >&2; exit 1; fi
  if [ "$mode" = append ] && "$HDFS" dfs -test -e "$dst"; then "$HDFS" dfs -appendToFile "$src" "$dst"
  else "$HDFS" dfs -put -f "$src" "$dst"; fi;;
 download)
  src=$1; dir=$2; mkdir -p "$dir"; name=$(basename "$src"); stem=$name; ext=
  if [[ "$name" = *.* && "$name" != .* ]]; then stem=${name%.*}; ext=.${name##*.}; fi
  dst=$dir/$name; n=0
  while [ -e "$dst" ]; do n=$((n+1)); dst="$dir/${stem}_${n}${ext}"; done
  "$HDFS" dfs -get "$src" "$dst"; printf '%s\n' "$dst";;
 cat) "$HDFS" dfs -cat "$1";;
 stat) "$HDFS" dfs -ls "$1"; "$HDFS" dfs -stat 'bytes=%b mtime=%y path=%n; creation time unavailable in HDFS' "$1";;
 list) "$HDFS" dfs -ls -R "$1";;
 touch) "$HDFS" dfs -mkdir -p "$(dirname "$1")"; if "$HDFS" dfs -test -e "$1"; then echo 'File exists' >&2; exit 1; fi; "$HDFS" dfs -touchz "$1";;
 mkdir) "$HDFS" dfs -mkdir -p "$1";;
 rmdir) "$HDFS" dfs -rmdir "$1";;
 append)
  dst=$1; src=$2; position=$3
  "$HDFS" dfs -test -f "$dst"
  case "$position" in
   tail) "$HDFS" dfs -appendToFile "$src" "$dst";;
   head)
    temp=$(mktemp); trap 'rm -f -- "$temp"' EXIT
    cat "$src" > "$temp"; "$HDFS" dfs -cat "$dst" >> "$temp"
    # HDFS Shell has no direct prepend. Re-upload the joined stream in this single-user lab.
    "$HDFS" dfs -put -f "$temp" "$dst";;
   *) echo 'Position must be head or tail' >&2; exit 2;;
  esac;;
 rm) "$HDFS" dfs -test -f "$1"; "$HDFS" dfs -rm "$1";;
 mv) "$HDFS" dfs -test -f "$1"; if "$HDFS" dfs -test -e "$2"; then echo 'Destination exists' >&2; exit 1; fi; "$HDFS" dfs -mkdir -p "$(dirname "$2")"; "$HDFS" dfs -mv "$1" "$2";;
 *) echo "Unknown operation: $cmd" >&2; exit 2;;
esac
