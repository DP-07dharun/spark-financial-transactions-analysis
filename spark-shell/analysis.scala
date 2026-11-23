// Part II - Interactive analysis with the Spark Shell (RDD API only).
//
// Run inside hadoop-master:
//   spark-shell --master yarn -i ~/bigdata-project/analysis.scala
// (scripts/run-all.ps1 adds the event-log settings so the run shows up in the History Server.)

sc.setLogLevel("WARN")

def section(title: String): Unit = println(s"\n==================== $title ====================")
def time[T](label: String)(block: => T): T = {
  val t0 = System.nanoTime()
  val result = block
  println(f"[$label took ${(System.nanoTime() - t0) / 1e6}%.0f ms]")
  result
}

// ---------------------------------------------------------------------------------------------
section("Load the dataset")
sc.setJobDescription("Load - first() to read the header")
val data = sc.textFile("hdfs:///project/input/transactions.csv")
val header = data.first()
val transactions = data.filter(line => line != header)

println(s"Header      : $header")
println(s"Partitions  : ${data.getNumPartitions}")
sc.setJobDescription("Load - count() records")
println(s"Records     : ${transactions.count()}")

// Columns: 0 user_id, 1 transaction_id, 2 bank_account_id, 3 financial_institution_id, 4 date,
//          5 third_party, 6 category, 7 transaction_amount, 8 city, 9 payment_method, 10 currency
val fields = transactions.map(_.split(","))

// ---------------------------------------------------------------------------------------------
section("Task 1 - Total transaction amount per category")
sc.setJobDescription("Task 1 - Total amount per category")
val amountPerCategory = fields
  .map(f => (f(6), f(7).toDouble))            // narrow
  .reduceByKey(_ + _)                         // wide (shuffle)
  .sortBy(_._2, ascending = false)            // wide (range-partition shuffle)
time("Task 1") { amountPerCategory.collect() }
  .foreach { case (cat, total) => println(f"$cat%-15s $total%12.2f EUR") }
println("\nLineage of Task 1:")
println(amountPerCategory.toDebugString)

// ---------------------------------------------------------------------------------------------
section("Task 2 - Number of transactions per user")
sc.setJobDescription("Task 2 - Transactions per user")
val txPerUser = fields.map(f => (f(0), 1)).reduceByKey(_ + _)
val top10Users = time("Task 2") { txPerUser.sortBy(_._2, ascending = false).take(10) }
println(s"Distinct users: ${txPerUser.count()}")
println("Top 10 most active users:")
top10Users.foreach { case (user, n) => println(f"  $user%-8s $n%4d transactions") }

// ---------------------------------------------------------------------------------------------
section("Task 3 - Most popular third-party vendors")
sc.setJobDescription("Task 3 - Most popular vendors")
val vendorStats = fields
  .map(f => (f(5), (1, f(7).toDouble)))
  .reduceByKey { case ((c1, a1), (c2, a2)) => (c1 + c2, a1 + a2) }
time("Task 3") { vendorStats.sortBy(_._2._1, ascending = false).take(10) }
  .foreach { case (vendor, (n, total)) => println(f"$vendor%-20s $n%5d tx  $total%11.2f EUR") }

// ---------------------------------------------------------------------------------------------
section("Task 4 - Top spending cities")
sc.setJobDescription("Task 4 - Top spending cities")
time("Task 4") {
  fields.map(f => (f(8), f(7).toDouble)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(10)
}.zipWithIndex.foreach { case ((city, total), i) => println(f"${i + 1}%2d. $city%-12s $total%12.2f EUR") }

// ---------------------------------------------------------------------------------------------
section("Task 5 - Financial institutions handling the most transactions")
sc.setJobDescription("Task 5 - Institutions with most transactions")
time("Task 5") {
  fields.map(f => (f(3).toInt, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).collect()
}.foreach { case (inst, n) => println(f"Institution $inst%3d  $n%5d transactions") }

// ---------------------------------------------------------------------------------------------
section("Task 6 - Monthly spending trends")
sc.setJobDescription("Task 6 - Monthly spending trends")
val monthly = fields
  .map(f => (f(4).substring(0, 7), (f(7).toDouble, 1)))           // "yyyy-MM"
  .reduceByKey { case ((a1, c1), (a2, c2)) => (a1 + a2, c1 + c2) }
  .sortByKey()
val monthlyRows = time("Task 6") { monthly.collect() }
val maxTotal = monthlyRows.map(_._2._1).max
monthlyRows.foreach { case (month, (total, n)) =>
  val bar = "#" * math.round(total / maxTotal * 40).toInt
  println(f"$month  $total%11.2f EUR  $n%4d tx  avg ${total / n}%7.2f  $bar")
}
println("\nLineage of Task 6:")
println(monthly.toDebugString)

// ---------------------------------------------------------------------------------------------
section("Caching experiment")
// Every task above re-read the file from HDFS and re-split each line. Cache the parsed RDD once
// and run all six aggregations again to measure the difference.
def runAll(rdd: org.apache.spark.rdd.RDD[Array[String]]): Unit = {
  rdd.map(f => (f(6), f(7).toDouble)).reduceByKey(_ + _).collect()
  rdd.map(f => (f(0), 1)).reduceByKey(_ + _).collect()
  rdd.map(f => (f(5), 1)).reduceByKey(_ + _).collect()
  rdd.map(f => (f(8), f(7).toDouble)).reduceByKey(_ + _).collect()
  rdd.map(f => (f(3), 1)).reduceByKey(_ + _).collect()
  rdd.map(f => (f(4).substring(0, 7), f(7).toDouble)).reduceByKey(_ + _).collect()
}
sc.setJobDescription("Caching - 6 tasks without cache")
time("6 tasks, NO cache") { runAll(fields) }
val cached = fields.cache()
sc.setJobDescription("Caching - materialise cache")
time("cache() + count() to materialise") { cached.count() }
sc.setJobDescription("Caching - 6 tasks with cache")
time("6 tasks, WITH cache") { runAll(cached) }
println(s"Cached RDD storage level: ${cached.getStorageLevel.description}")
cached.unpersist()

println(s"\nApplication ID: ${sc.applicationId}")
println("Done.")
System.exit(0)
