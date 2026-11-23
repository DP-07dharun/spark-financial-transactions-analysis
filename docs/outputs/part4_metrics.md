# Spark execution metrics (from the History Server REST API)

## Part II - Spark Shell analysis

Application `application_1795467892423_0001` · duration 31.7 s · Spark 3.5.0

Executors: #1 on hadoop-worker2 (51 tasks), #2 on hadoop-worker1 (50 tasks)

| Job | Triggered by (action) | Stages run | Stages skipped | Tasks run |
|---|---|---|---|---|
| 0 | `first` at analysis.scala:43 — Load - first() to read the header | 0 | - | 1 |
| 1 | `count` at analysis.scala:49 — Load - count() records | 1 | - | 2 |
| 2 | `sortBy` at analysis.scala:61 — Task 1 - Total amount per category | 2, 3 | - | 4 |
| 3 | `collect` at analysis.scala:62 — Task 1 - Total amount per category | 5, 6 | 4 | 4 |
| 4 | `sortBy` at analysis.scala:71 — Task 2 - Transactions per user | 7, 8 | - | 4 |
| 5 | `take` at analysis.scala:71 — Task 2 - Transactions per user | 10, 11 | 9 | 3 |
| 6 | `count` at analysis.scala:72 — Task 2 - Transactions per user | 13 | 12 | 2 |
| 7 | `sortBy` at analysis.scala:82 — Task 3 - Most popular vendors | 15, 14 | - | 4 |
| 8 | `take` at analysis.scala:82 — Task 3 - Most popular vendors | 17, 18 | 16 | 3 |
| 9 | `sortBy` at analysis.scala:89 — Task 4 - Top spending cities | 19, 20 | - | 4 |
| 10 | `take` at analysis.scala:89 — Task 4 - Top spending cities | 22, 23 | 21 | 3 |
| 11 | `take` at analysis.scala:89 — Task 4 - Top spending cities | 26 | 24, 25 | 1 |
| 12 | `sortBy` at analysis.scala:96 — Task 5 - Institutions with most transactions | 27, 28 | - | 4 |
| 13 | `collect` at analysis.scala:96 — Task 5 - Institutions with most transactions | 30, 31 | 29 | 4 |
| 14 | `sortByKey` at analysis.scala:105 — Task 6 - Monthly spending trends | 33, 32 | - | 4 |
| 15 | `collect` at analysis.scala:106 — Task 6 - Monthly spending trends | 35, 36 | 34 | 4 |
| 16 | `collect` at analysis.scala:120 — Caching - 6 tasks without cache | 37, 38 | - | 4 |
| 17 | `collect` at analysis.scala:121 — Caching - 6 tasks without cache | 39, 40 | - | 4 |
| 18 | `collect` at analysis.scala:122 — Caching - 6 tasks without cache | 42, 41 | - | 4 |
| 19 | `collect` at analysis.scala:123 — Caching - 6 tasks without cache | 43, 44 | - | 4 |
| 20 | `collect` at analysis.scala:124 — Caching - 6 tasks without cache | 45, 46 | - | 4 |
| 21 | `collect` at analysis.scala:125 — Caching - 6 tasks without cache | 48, 47 | - | 4 |
| 22 | `count` at analysis.scala:131 — Caching - materialise cache | 49 | - | 2 |
| 23 | `collect` at analysis.scala:120 — Caching - 6 tasks with cache | 51, 50 | - | 4 |
| 24 | `collect` at analysis.scala:121 — Caching - 6 tasks with cache | 52, 53 | - | 4 |
| 25 | `collect` at analysis.scala:122 — Caching - 6 tasks with cache | 54, 55 | - | 4 |
| 26 | `collect` at analysis.scala:123 — Caching - 6 tasks with cache | 56, 57 | - | 4 |
| 27 | `collect` at analysis.scala:124 — Caching - 6 tasks with cache | 58, 59 | - | 4 |
| 28 | `collect` at analysis.scala:125 — Caching - 6 tasks with cache | 60, 61 | - | 4 |

**Total: 29 jobs, 53 stages executed, 9 skipped, 101 tasks.**

| Stage | Name | Tasks | Input | Shuffle write | Shuffle read | Run time |
|---|---|---|---|---|---|---|
| 0 | `first at analysis.scala:43` | 1 | 64.0 KB | - | - | 550 ms |
| 1 | `count at analysis.scala:49` | 2 | 486.8 KB | - | - | 751 ms |
| 2 | `map at analysis.scala:59` | 2 | 486.8 KB | 0.5 KB | - | 629 ms |
| 3 | `sortBy at analysis.scala:61` | 2 | - | - | 0.5 KB | 226 ms |
| 5 | `sortBy at analysis.scala:61` | 2 | - | 0.8 KB | 0.5 KB | 55 ms |
| 6 | `collect at analysis.scala:62` | 2 | - | - | 0.8 KB | 28 ms |
| 7 | `map at analysis.scala:70` | 2 | 486.8 KB | 2.8 KB | - | 121 ms |
| 8 | `sortBy at analysis.scala:71` | 2 | - | - | 2.8 KB | 39 ms |
| 10 | `sortBy at analysis.scala:71` | 2 | - | 4.0 KB | 2.8 KB | 38 ms |
| 11 | `take at analysis.scala:71` | 1 | - | - | 2.0 KB | 16 ms |
| 13 | `count at analysis.scala:72` | 2 | - | - | 2.8 KB | 26 ms |
| 14 | `map at analysis.scala:80` | 2 | 486.8 KB | 2.6 KB | - | 47 ms |
| 15 | `sortBy at analysis.scala:82` | 2 | - | - | 2.6 KB | 20 ms |
| 17 | `sortBy at analysis.scala:82` | 2 | - | 2.3 KB | 2.6 KB | 22 ms |
| 18 | `take at analysis.scala:82` | 1 | - | - | 1.1 KB | 9 ms |
| 19 | `map at analysis.scala:89` | 2 | 486.8 KB | 0.5 KB | - | 42 ms |
| 20 | `sortBy at analysis.scala:89` | 2 | - | - | 0.5 KB | 21 ms |
| 22 | `sortBy at analysis.scala:89` | 2 | - | 1.0 KB | 0.5 KB | 21 ms |
| 23 | `take at analysis.scala:89` | 1 | - | - | 0.5 KB | 8 ms |
| 26 | `take at analysis.scala:89` | 1 | - | - | 0.5 KB | 5 ms |
| 27 | `map at analysis.scala:96` | 2 | 486.8 KB | 0.3 KB | - | 52 ms |
| 28 | `sortBy at analysis.scala:96` | 2 | - | - | 0.3 KB | 21 ms |
| 30 | `sortBy at analysis.scala:96` | 2 | - | 0.9 KB | 0.3 KB | 23 ms |
| 31 | `collect at analysis.scala:96` | 2 | - | - | 0.9 KB | 17 ms |
| 32 | `map at analysis.scala:103` | 2 | 486.8 KB | 0.9 KB | - | 37 ms |
| 33 | `sortByKey at analysis.scala:105` | 2 | - | - | 0.9 KB | 19 ms |
| 35 | `reduceByKey at analysis.scala:104` | 2 | - | 0.9 KB | 0.9 KB | 23 ms |
| 36 | `collect at analysis.scala:106` | 2 | - | - | 0.9 KB | 29 ms |
| 37 | `map at analysis.scala:120` | 2 | 486.8 KB | 0.5 KB | - | 45 ms |
| 38 | `collect at analysis.scala:120` | 2 | - | - | 0.5 KB | 30 ms |
| 39 | `map at analysis.scala:121` | 2 | 486.8 KB | 2.8 KB | - | 54 ms |
| 40 | `collect at analysis.scala:121` | 2 | - | - | 2.8 KB | 28 ms |
| 41 | `map at analysis.scala:122` | 2 | 486.8 KB | 1.2 KB | - | 39 ms |
| 42 | `collect at analysis.scala:122` | 2 | - | - | 1.2 KB | 20 ms |
| 43 | `map at analysis.scala:123` | 2 | 486.8 KB | 0.5 KB | - | 50 ms |
| 44 | `collect at analysis.scala:123` | 2 | - | - | 0.5 KB | 22 ms |
| 45 | `map at analysis.scala:124` | 2 | 486.8 KB | 0.3 KB | - | 58 ms |
| 46 | `collect at analysis.scala:124` | 2 | - | - | 0.3 KB | 28 ms |
| 47 | `map at analysis.scala:125` | 2 | 486.8 KB | 0.4 KB | - | 89 ms |
| 48 | `collect at analysis.scala:125` | 2 | - | - | 0.4 KB | 36 ms |
| 49 | `count at analysis.scala:131` | 2 | 486.8 KB | - | - | 213 ms |
| 50 | `map at analysis.scala:120` | 2 | 3454.6 KB | 0.5 KB | - | 16 ms |
| 51 | `collect at analysis.scala:120` | 2 | - | - | 0.5 KB | 14 ms |
| 52 | `map at analysis.scala:121` | 2 | 3454.6 KB | 2.8 KB | - | 15 ms |
| 53 | `collect at analysis.scala:121` | 2 | - | - | 2.8 KB | 16 ms |
| 54 | `map at analysis.scala:122` | 2 | 3454.6 KB | 1.2 KB | - | 11 ms |
| 55 | `collect at analysis.scala:122` | 2 | - | - | 1.2 KB | 12 ms |
| 56 | `map at analysis.scala:123` | 2 | 3454.6 KB | 0.5 KB | - | 11 ms |
| 57 | `collect at analysis.scala:123` | 2 | - | - | 0.5 KB | 14 ms |
| 58 | `map at analysis.scala:124` | 2 | 3454.6 KB | 0.3 KB | - | 8 ms |
| 59 | `collect at analysis.scala:124` | 2 | - | - | 0.3 KB | 12 ms |
| 60 | `map at analysis.scala:125` | 2 | 3454.6 KB | 0.4 KB | - | 10 ms |
| 61 | `collect at analysis.scala:125` | 2 | - | - | 0.4 KB | 12 ms |

## Task 1 - CategorySpending

Application `application_1795467892423_0008` · duration 19.5 s · Spark 3.5.0

Executors: #1 on hadoop-worker2 (5 tasks), #2 on hadoop-worker1 (5 tasks)

| Job | Triggered by (action) | Stages run | Stages skipped | Tasks run |
|---|---|---|---|---|
| 0 | `first` at Transactions.java:84 | 0 | - | 1 |
| 1 | `sortByKey` at CategorySpending.java:22 | 1, 2 | - | 4 |
| 2 | `runJob` at SparkHadoopWriter.scala:83 | 5, 4 | 3 | 3 |
| 3 | `collect` at CategorySpending.java:28 | 8 | 6, 7 | 2 |

**Total: 4 jobs, 6 stages executed, 3 skipped, 10 tasks.**

| Stage | Name | Tasks | Input | Shuffle write | Shuffle read | Run time |
|---|---|---|---|---|---|---|
| 0 | `first at Transactions.java:84` | 1 | 64.0 KB | - | - | 598 ms |
| 1 | `mapToPair at CategorySpending.java:19` | 2 | 486.8 KB | 0.8 KB | - | 866 ms |
| 2 | `sortByKey at CategorySpending.java:22` | 2 | - | - | 0.8 KB | 215 ms |
| 4 | `mapToPair at CategorySpending.java:21` | 2 | - | 0.5 KB | 0.8 KB | 38 ms |
| 5 | `runJob at SparkHadoopWriter.scala:83` | 1 | - | - | 0.5 KB | 130 ms |
| 8 | `collect at CategorySpending.java:28` | 2 | - | - | 0.5 KB | 19 ms |

## Task 2 - TransactionsPerUser

Application `application_1795467892423_0009` · duration 20.6 s · Spark 3.5.0

Executors: #1 on hadoop-worker1 (9 tasks), #2 on hadoop-worker2 (8 tasks)

| Job | Triggered by (action) | Stages run | Stages skipped | Tasks run |
|---|---|---|---|---|
| 0 | `first` at Transactions.java:84 | 0 | - | 1 |
| 1 | `sortByKey` at TransactionsPerUser.java:24 | 1, 2 | - | 4 |
| 2 | `runJob` at SparkHadoopWriter.scala:83 | 5, 4 | 3 | 3 |
| 3 | `sortByKey` at TransactionsPerUser.java:26 | 7 | 6 | 2 |
| 4 | `take` at TransactionsPerUser.java:26 | 9, 10 | 8 | 3 |
| 5 | `count` at TransactionsPerUser.java:27 | 12 | 11 | 2 |
| 6 | `mean` at TransactionsPerUser.java:28 | 14 | 13 | 2 |

**Total: 7 jobs, 10 stages executed, 5 skipped, 17 tasks.**

| Stage | Name | Tasks | Input | Shuffle write | Shuffle read | Run time |
|---|---|---|---|---|---|---|
| 0 | `first at Transactions.java:84` | 1 | 64.0 KB | - | - | 735 ms |
| 1 | `mapToPair at TransactionsPerUser.java:19` | 2 | 486.8 KB | 4.1 KB | - | 898 ms |
| 2 | `sortByKey at TransactionsPerUser.java:24` | 2 | - | - | 4.1 KB | 232 ms |
| 4 | `reduceByKey at TransactionsPerUser.java:20` | 2 | - | 2.5 KB | 4.1 KB | 45 ms |
| 5 | `runJob at SparkHadoopWriter.scala:83` | 1 | - | - | 2.5 KB | 150 ms |
| 7 | `sortByKey at TransactionsPerUser.java:26` | 2 | - | - | 4.1 KB | 28 ms |
| 9 | `mapToPair at TransactionsPerUser.java:26` | 2 | - | 2.5 KB | 4.1 KB | 28 ms |
| 10 | `take at TransactionsPerUser.java:26` | 1 | - | - | 1.3 KB | 15 ms |
| 12 | `count at TransactionsPerUser.java:27` | 2 | - | - | 4.1 KB | 22 ms |
| 14 | `mean at TransactionsPerUser.java:28` | 2 | - | - | 4.1 KB | 19 ms |

## Task 3 - TopVendors

Application `application_1795467892423_0010` · duration 18.4 s · Spark 3.5.0

Executors: #1 on hadoop-worker2 (6 tasks), #2 on hadoop-worker1 (3 tasks)

| Job | Triggered by (action) | Stages run | Stages skipped | Tasks run |
|---|---|---|---|---|
| 0 | `first` at Transactions.java:84 | 0 | - | 1 |
| 1 | `sortByKey` at TopVendors.java:26 | 1, 2 | - | 4 |
| 2 | `runJob` at SparkHadoopWriter.scala:83 | 5, 4 | 3 | 3 |
| 3 | `take` at TopVendors.java:32 | 8 | 6, 7 | 1 |

**Total: 4 jobs, 6 stages executed, 3 skipped, 9 tasks.**

| Stage | Name | Tasks | Input | Shuffle write | Shuffle read | Run time |
|---|---|---|---|---|---|---|
| 0 | `first at Transactions.java:84` | 1 | 64.0 KB | - | - | 603 ms |
| 1 | `mapToPair at TopVendors.java:21` | 2 | 486.8 KB | 3.2 KB | - | 853 ms |
| 2 | `sortByKey at TopVendors.java:26` | 2 | - | - | 3.2 KB | 195 ms |
| 4 | `mapToPair at TopVendors.java:25` | 2 | - | 2.1 KB | 3.2 KB | 40 ms |
| 5 | `runJob at SparkHadoopWriter.scala:83` | 1 | - | - | 2.1 KB | 124 ms |
| 8 | `take at TopVendors.java:32` | 1 | - | - | 1.0 KB | 6 ms |

## Task 4 - TopCities

Application `application_1795467892423_0011` · duration 18.1 s · Spark 3.5.0

Executors: #1 on hadoop-worker1 (6 tasks), #2 on hadoop-worker2 (4 tasks)

| Job | Triggered by (action) | Stages run | Stages skipped | Tasks run |
|---|---|---|---|---|
| 0 | `first` at Transactions.java:84 | 0 | - | 1 |
| 1 | `sortByKey` at TopCities.java:22 | 1, 2 | - | 4 |
| 2 | `runJob` at SparkHadoopWriter.scala:83 | 5, 4 | 3 | 3 |
| 3 | `take` at TopCities.java:27 | 8 | 6, 7 | 1 |
| 4 | `take` at TopCities.java:27 | 11 | 9, 10 | 1 |

**Total: 5 jobs, 7 stages executed, 5 skipped, 10 tasks.**

| Stage | Name | Tasks | Input | Shuffle write | Shuffle read | Run time |
|---|---|---|---|---|---|---|
| 0 | `first at Transactions.java:84` | 1 | 64.0 KB | - | - | 614 ms |
| 1 | `mapToPair at TopCities.java:19` | 2 | 486.8 KB | 0.8 KB | - | 748 ms |
| 2 | `sortByKey at TopCities.java:22` | 2 | - | - | 0.8 KB | 184 ms |
| 4 | `mapToPair at TopCities.java:21` | 2 | - | 0.6 KB | 0.8 KB | 38 ms |
| 5 | `runJob at SparkHadoopWriter.scala:83` | 1 | - | - | 0.6 KB | 129 ms |
| 8 | `take at TopCities.java:27` | 1 | - | - | 0.3 KB | 15 ms |
| 11 | `take at TopCities.java:27` | 1 | - | - | 0.3 KB | 6 ms |

## Task 5 - InstitutionWithMostTransactions

Application `application_1795467892423_0012` · duration 17.5 s · Spark 3.5.0

Executors: #1 on hadoop-worker1 (4 tasks), #2 on hadoop-worker2 (6 tasks)

| Job | Triggered by (action) | Stages run | Stages skipped | Tasks run |
|---|---|---|---|---|
| 0 | `first` at Transactions.java:84 | 0 | - | 1 |
| 1 | `sortByKey` at InstitutionWithMostTransactions.java:22 | 1, 2 | - | 4 |
| 2 | `runJob` at SparkHadoopWriter.scala:83 | 5, 4 | 3 | 3 |
| 3 | `collect` at InstitutionWithMostTransactions.java:27 | 8 | 6, 7 | 2 |

**Total: 4 jobs, 6 stages executed, 3 skipped, 10 tasks.**

| Stage | Name | Tasks | Input | Shuffle write | Shuffle read | Run time |
|---|---|---|---|---|---|---|
| 0 | `first at Transactions.java:84` | 1 | 64.0 KB | - | - | 575 ms |
| 1 | `mapToPair at InstitutionWithMostTransactions.java:19` | 2 | 486.8 KB | 0.6 KB | - | 757 ms |
| 2 | `sortByKey at InstitutionWithMostTransactions.java:22` | 2 | - | - | 0.6 KB | 207 ms |
| 4 | `mapToPair at InstitutionWithMostTransactions.java:21` | 2 | - | 0.6 KB | 0.6 KB | 33 ms |
| 5 | `runJob at SparkHadoopWriter.scala:83` | 1 | - | - | 0.6 KB | 121 ms |
| 8 | `collect at InstitutionWithMostTransactions.java:27` | 2 | - | - | 0.6 KB | 20 ms |

## Task 6 - MonthlySpendingTrends

Application `application_1795467892423_0013` · duration 16.5 s · Spark 3.5.0

Executors: #1 on hadoop-worker2 (6 tasks), #2 on hadoop-worker1 (4 tasks)

| Job | Triggered by (action) | Stages run | Stages skipped | Tasks run |
|---|---|---|---|---|
| 0 | `first` at Transactions.java:84 | 0 | - | 1 |
| 1 | `sortByKey` at MonthlySpendingTrends.java:21 | 1, 2 | - | 4 |
| 2 | `runJob` at SparkHadoopWriter.scala:83 | 5, 4 | 3 | 3 |
| 3 | `collect` at MonthlySpendingTrends.java:27 | 8 | 6, 7 | 2 |

**Total: 4 jobs, 6 stages executed, 3 skipped, 10 tasks.**

| Stage | Name | Tasks | Input | Shuffle write | Shuffle read | Run time |
|---|---|---|---|---|---|---|
| 0 | `first at Transactions.java:84` | 1 | 64.0 KB | - | - | 548 ms |
| 1 | `mapToPair at MonthlySpendingTrends.java:19` | 2 | 486.8 KB | 1.2 KB | - | 786 ms |
| 2 | `sortByKey at MonthlySpendingTrends.java:21` | 2 | - | - | 1.2 KB | 186 ms |
| 4 | `reduceByKey at MonthlySpendingTrends.java:20` | 2 | - | 1.1 KB | 1.2 KB | 36 ms |
| 5 | `runJob at SparkHadoopWriter.scala:83` | 1 | - | - | 1.1 KB | 124 ms |
| 8 | `collect at MonthlySpendingTrends.java:27` | 2 | - | - | 1.1 KB | 23 ms |
