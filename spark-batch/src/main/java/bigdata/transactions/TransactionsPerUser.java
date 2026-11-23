package bigdata.transactions;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/** Task 2 - Number of transactions per user. */
public final class TransactionsPerUser {

    public static void main(String[] args) throws Exception {
        String input = Transactions.inputPath(args);
        String output = Transactions.outputPath(args, "transactions_per_user");

        try (JavaSparkContext sc = Transactions.context("Task 2 - TransactionsPerUser")) {
            JavaPairRDD<String, Integer> perUser = Transactions.load(sc, input)
                    .mapToPair(t -> new Tuple2<>(t.userId, 1))
                    .reduceByKey(Integer::sum);

            // Full result (sorted by user id) goes to HDFS; the console shows the top 10.
            Transactions.deleteIfExists(sc, output);
            perUser.sortByKey().map(t -> t._1 + "," + t._2).coalesce(1).saveAsTextFile(output);

            List<Tuple2<Integer, String>> top = perUser.mapToPair(Tuple2::swap).sortByKey(false).take(10);
            long users = perUser.count();
            double average = perUser.values().mapToDouble(Integer::doubleValue).mean();

            Transactions.banner("Task 2 - Number of transactions per user");
            System.out.printf("Distinct users: %d, average %.1f transactions per user%n", users, average);
            System.out.println("Top 10 most active users:");
            for (Tuple2<Integer, String> r : top) {
                System.out.printf("  %-8s %4d transactions%n", r._2, r._1);
            }
            System.out.println("Saved to " + output);
        }
    }
}
