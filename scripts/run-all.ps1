<#
.SYNOPSIS
  Run the whole project end to end on the Docker Hadoop/Spark cluster (Windows PowerShell).

.DESCRIPTION
  1. Generates data/transactions.csv (5000 rows, fixed seed).
  2. Starts hadoop-master / hadoop-worker1 / hadoop-worker2 (creating them from
     liliasfaxi/hadoop-cluster if they do not exist), then HDFS + YARN.
  3. Starts the Spark History Server and a small port-forward container for the Spark UI.
  4. Part I   - loads the CSV into HDFS            -> docs/outputs/part1_hdfs.txt
  5. Part II  - Spark Shell analysis on YARN       -> docs/outputs/part2_spark_shell.txt
  6. Part III - builds and submits the Java jobs   -> docs/outputs/part3_batch_jobs*.txt
  7. Part IV  - collects stage/task metrics        -> docs/outputs/part4_metrics.md

  Requirements: Docker Desktop, Python 3, Maven 3.9+ and a JDK 11+ on PATH (or -Maven <path to mvn>).

.EXAMPLE
  .\scripts\run-all.ps1
  .\scripts\run-all.ps1 -Maven D:\tools\apache-maven-3.10.2\bin\mvn.cmd -SkipShell
#>
param(
  [string]$Maven = "mvn",
  [string]$MavenRepo = "",
  [switch]$SkipShell,
  [switch]$SkipBatch
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Out = Join-Path $Root "docs\outputs"
New-Item -ItemType Directory -Force $Out | Out-Null
$Remote = "/root/bigdata-project"

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function InMaster([string]$cmd) { docker exec hadoop-master bash -c $cmd }
function CopyToMaster([string]$path) {
  docker cp $path "hadoop-master:$Remote/" | Out-Null
  $name = Split-Path $path -Leaf
  InMaster "sed -i 's/\r$//' $Remote/$name" | Out-Null   # CRLF -> LF for shell/Scala scripts
}

Step "Generating the dataset"
python (Join-Path $Root "data\generate_transactions.py")

Step "Starting the cluster containers"
$existing = docker ps -a --format "{{.Names}}"
if ($existing -notcontains "hadoop-master") {
  if ((docker network ls --format "{{.Name}}") -notcontains "hadoop") { docker network create --driver=bridge hadoop | Out-Null }
  docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest | Out-Null
  docker run -itd -p 8040:8042 --net=hadoop --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest | Out-Null
  docker run -itd -p 8041:8042 --net=hadoop --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest | Out-Null
} else {
  docker start hadoop-master hadoop-worker1 hadoop-worker2 | Out-Null
}
InMaster "mkdir -p $Remote"
if (-not (InMaster "jps | grep -c NameNode")) { InMaster "./start-hadoop.sh >/dev/null 2>&1; hdfs dfsadmin -safemode wait" }

Step "Spark History Server + UI port forward (4040, 18080)"
CopyToMaster (Join-Path $Root "cluster\start-history-server.sh")
if (-not (InMaster "jps | grep -c HistoryServer")) { InMaster "bash $Remote/start-history-server.sh" }
if ((docker ps -a --format "{{.Names}}") -notcontains "spark-ui-proxy") {
  docker run -d --name spark-ui-proxy --network hadoop -p 4040:4040 -p 18080:18080 --entrypoint sh alpine/socat `
    -c "socat TCP-LISTEN:4040,fork,reuseaddr TCP:hadoop-master:4040 & socat TCP-LISTEN:18080,fork,reuseaddr TCP:hadoop-master:18080" | Out-Null
} else { docker start spark-ui-proxy | Out-Null }

Step "Shared Spark jar archive (spark.yarn.archive) - avoids re-uploading ~320 MB of jars per job"
if (-not (InMaster "grep -c '^spark.yarn.archive' /usr/local/spark/conf/spark-defaults.conf 2>/dev/null")) {
  CopyToMaster (Join-Path $Root "cluster\setup-spark-yarn-archive.sh")
  InMaster "bash $Remote/setup-spark-yarn-archive.sh"
}

Step "Part I - HDFS"
docker cp (Join-Path $Root "data\transactions.csv") "hadoop-master:$Remote/transactions.csv" | Out-Null
CopyToMaster (Join-Path $Root "cluster\part1_hdfs.sh")
InMaster "bash $Remote/part1_hdfs.sh 2>&1 | grep -v -E '^WARNING|SLF4J'" | Out-File -Encoding utf8 "$Out\part1_hdfs.txt"
Get-Content "$Out\part1_hdfs.txt" -Tail 3

if (-not $SkipShell) {
  Step "Part II - Spark Shell analysis"
  CopyToMaster (Join-Path $Root "spark-shell\analysis.scala")
  CopyToMaster (Join-Path $Root "cluster\run-spark-shell.sh")
  $raw = InMaster "bash $Remote/run-spark-shell.sh 2>&1"
  (($raw -join "`n") -replace '\[Stage [^\n]*?\]\s*', '') -split "`n" | Where-Object { $_.Trim() } |
    Out-File -Encoding utf8 "$Out\part2_spark_shell.txt"
  Get-Content "$Out\part2_spark_shell.txt" -Tail 5
}

if (-not $SkipBatch) {
  Step "Part III - build the Java jobs"
  Push-Location (Join-Path $Root "spark-batch")
  $mvnArgs = @("-q", "-B", "package")
  if ($MavenRepo) { $mvnArgs = @("-Dmaven.repo.local=$MavenRepo") + $mvnArgs }
  & $Maven @mvnArgs
  if ($LASTEXITCODE -ne 0) { throw "Maven build failed" }
  Pop-Location

  Step "Part III - submit the six jobs to YARN"
  docker cp (Join-Path $Root "spark-batch\target\spark-transactions-batch.jar") "hadoop-master:$Remote/" | Out-Null
  CopyToMaster (Join-Path $Root "cluster\run-batch-jobs.sh")
  CopyToMaster (Join-Path $Root "cluster\show-output.sh")
  InMaster "bash $Remote/run-batch-jobs.sh" | Out-File -Encoding utf8 "$Out\part3_batch_jobs_full_log.txt"
  $keep = $false
  Get-Content "$Out\part3_batch_jobs_full_log.txt" | ForEach-Object {
    if ($_ -match '^# spark-submit|^={5,} Task|^### ') { $keep = $true }
    if ($_ -match '^\d\d/\d\d/\d\d ') { $keep = $false }
    if ($keep -or $_ -match 'finished in|^=== HDFS|^-rw|^drwx') { $_ }
  } | Out-File -Encoding utf8 "$Out\part3_batch_jobs.txt"
  InMaster "bash $Remote/show-output.sh" | Out-File -Encoding utf8 "$Out\hdfs_output_files.txt"
  Select-String -Path "$Out\part3_batch_jobs.txt" -Pattern "finished in" | ForEach-Object { $_.Line }
}

Step "Part IV - execution metrics from the History Server"
Start-Sleep -Seconds 8   # let the History Server pick up the latest event logs
python (Join-Path $Root "scripts\spark_metrics.py") --out "$Out\part4_metrics.md"

Write-Host "`nDone. Spark History Server: http://localhost:18080 | HDFS: http://localhost:9870 | YARN: http://localhost:8088" -ForegroundColor Green
