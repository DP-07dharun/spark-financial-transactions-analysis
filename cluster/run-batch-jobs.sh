#!/usr/bin/env bash
# Part III - submit every Java batch job to YARN, one application per analytical task.
# Run inside hadoop-master after copying target/spark-transactions-batch.jar to ~/bigdata-project/.
set -uo pipefail
cd "$HOME/bigdata-project"
JAR=spark-transactions-batch.jar
CLASSES=(CategorySpending TransactionsPerUser TopVendors TopCities InstitutionWithMostTransactions MonthlySpendingTrends)
[ $# -gt 0 ] && CLASSES=("$@")

for cls in "${CLASSES[@]}"; do
  echo
  echo "################################################################################"
  echo "# spark-submit --class bigdata.transactions.$cls $JAR"
  echo "################################################################################"
  start=$(date +%s%N)
  spark-submit \
    --master yarn \
    --deploy-mode client \
    --class "bigdata.transactions.$cls" \
    --driver-memory 1g \
    --executor-memory 1g \
    --num-executors 2 \
    --executor-cores 1 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///spark-logs \
    "$JAR" 2>&1 | grep -v -E '^\s*$|WARN NativeCodeLoader|WARN Client: Neither|^\[Stage'
  status=${PIPESTATUS[0]}
  elapsed_ms=$(( ($(date +%s%N) - start) / 1000000 ))
  printf '[%s finished in %d.%03d s (spark-submit wall time), exit %s]\n' "$cls" $((elapsed_ms / 1000)) $((elapsed_ms % 1000)) "$status"
done

echo
echo "=== HDFS output directories ==="
hdfs dfs -ls -R /project/output 2>/dev/null | grep -v _SUCCESS
