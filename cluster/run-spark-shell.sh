#!/usr/bin/env bash
# Part II - run the Spark Shell analysis on YARN (2 executors, one per worker). Run inside hadoop-master.
set -euo pipefail
cd "$HOME/bigdata-project"
spark-shell \
  --master yarn \
  --name "Part II - Spark Shell analysis" \
  --driver-memory 1g \
  --executor-memory 1g \
  --num-executors 2 \
  --executor-cores 1 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs:///spark-logs \
  -i analysis.scala
