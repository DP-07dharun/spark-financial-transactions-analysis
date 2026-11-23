#!/usr/bin/env bash
# One-time setup: upload Spark's jars to HDFS once and point spark.yarn.archive at them.
#
# Without it, every spark-submit / spark-shell on YARN uploads ~320 MB of jars to HDFS
# ("Neither spark.yarn.jars nor spark.yarn.archive is set") and every NodeManager keeps a
# private copy per application, so each run costs time and ~0.3 GB of disk per worker.
# With a world-readable archive, YARN localises it once per node and reuses it.
# Run inside hadoop-master.
set -euo pipefail
SPARK_HOME="${SPARK_HOME:-/usr/local/spark}"
ARCHIVE_DIR=/spark-jars
ARCHIVE="$ARCHIVE_DIR/spark-libs.jar"
CONF="$SPARK_HOME/conf/spark-defaults.conf"

tmp=$(mktemp -d)
echo "Packing $SPARK_HOME/jars (uncompressed jar, as Spark recommends)..."
(cd "$SPARK_HOME/jars" && jar cv0f "$tmp/spark-libs.jar" ./*.jar >/dev/null)

hdfs dfs -mkdir -p "$ARCHIVE_DIR"
hdfs dfs -put -f "$tmp/spark-libs.jar" "$ARCHIVE"
hdfs dfs -chmod 755 "$ARCHIVE_DIR"
hdfs dfs -chmod 644 "$ARCHIVE"   # public visibility -> shared NodeManager cache
rm -rf "$tmp"
hdfs dfs -ls "$ARCHIVE_DIR"

if grep -q '^spark.yarn.archive' "$CONF"; then
  sed -i "s#^spark.yarn.archive.*#spark.yarn.archive\t\thdfs://$ARCHIVE#" "$CONF"
else
  [ -f "$CONF.bak" ] || cp "$CONF" "$CONF.bak"
  printf 'spark.yarn.archive\t\thdfs://%s\n' "$ARCHIVE" >> "$CONF"
fi
echo "spark-defaults.conf:"; grep -v '^#' "$CONF" | grep -v '^$'
