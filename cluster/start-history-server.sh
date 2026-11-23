#!/usr/bin/env bash
# Start the Spark History Server so finished applications stay visible in the Spark Web UI
# (http://localhost:18080 through the spark-ui-proxy container). Run inside hadoop-master.
set -euo pipefail
export SPARK_HOME="${SPARK_HOME:-/usr/local/spark}"
hdfs dfs -mkdir -p /spark-logs
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs:///spark-logs -Dspark.history.fs.update.interval=5s -Dspark.history.ui.port=18080"
"$SPARK_HOME/sbin/start-history-server.sh"
sleep 5
jps | grep -i HistoryServer && echo "History server running on :18080"
