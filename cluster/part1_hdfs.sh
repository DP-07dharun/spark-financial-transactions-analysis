#!/usr/bin/env bash
# Part I - Data preparation in HDFS. Run inside hadoop-master:
#   bash ~/bigdata-project/part1_hdfs.sh
# Expects the CSV at ~/bigdata-project/transactions.csv (copied there by scripts/run-all.ps1).
set -euo pipefail
LOCAL_CSV="${1:-$HOME/bigdata-project/transactions.csv}"

step() { printf '\n$ %s\n' "$*"; "$@"; }

echo "=== Task 1 - Create the directory project/input ==="
step hdfs dfs -mkdir -p /project/input

echo; echo "=== Task 2 - Upload the CSV file to HDFS ==="
step hdfs dfs -put -f "$LOCAL_CSV" /project/input/transactions.csv

echo; echo "=== Task 3 - Verify the upload ==="
step hdfs dfs -ls /project/input
step hdfs dfs -du -h /project/input/transactions.csv
step hdfs fsck /project/input/transactions.csv -files -blocks -locations

echo; echo "=== Task 4 - Display the first records ==="
printf '\n$ hdfs dfs -cat /project/input/transactions.csv | head -n 11\n'
hdfs dfs -cat /project/input/transactions.csv 2>/dev/null | head -n 11 || true
printf '\n$ hdfs dfs -cat /project/input/transactions.csv | wc -l\n'
hdfs dfs -cat /project/input/transactions.csv | wc -l
