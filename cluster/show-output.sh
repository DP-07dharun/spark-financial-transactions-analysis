#!/usr/bin/env bash
# Print the result files written to HDFS by the Java batch jobs. Run inside hadoop-master.
for d in category_spending transactions_per_user top_vendors top_cities institutions monthly_trends; do
  echo
  echo "\$ hdfs dfs -cat /project/output/$d/part-00000 | head -n 12"
  hdfs dfs -cat "/project/output/$d/part-00000" 2>/dev/null | head -n 12
done
