package bigdata.transactions;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/** Task 3 - Most popular third-party vendors (by number of transactions, with amount spent). */
public final class TopVendors {

    public static void main(String[] args) throws Exception {
        String input = Transactions.inputPath(args);
        String output = Transactions.outputPath(args, "top_vendors");
        int topN = 10;

        try (JavaSparkContext sc = Transactions.context("Task 3 - TopVendors")) {
            // (vendor, (count, amount)) aggregated in a single shuffle.
            JavaPairRDD<String, Tuple2<Integer, Double>> stats = Transactions.load(sc, input)
                    .mapToPair(t -> new Tuple2<>(t.thirdParty, new Tuple2<>(1, t.amount)))
                    .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

            JavaPairRDD<Integer, Tuple2<String, Double>> byCount = stats
                    .mapToPair(t -> new Tuple2<>(t._2._1, new Tuple2<>(t._1, t._2._2)))
                    .sortByKey(false);

            Transactions.deleteIfExists(sc, output);
            byCount.map(t -> t._2._1 + "," + t._1 + "," + String.format("%.2f", t._2._2))
                    .coalesce(1).saveAsTextFile(output);

            List<Tuple2<Integer, Tuple2<String, Double>>> top = byCount.take(topN);
            Transactions.banner("Task 3 - Most popular third-party vendors (top " + topN + ")");
            int rank = 1;
            for (Tuple2<Integer, Tuple2<String, Double>> r : top) {
                System.out.printf("%2d. %-20s %5d tx %11.2f EUR%n", rank++, r._2._1, r._1, r._2._2);
            }
            System.out.println("Saved to " + output);
        }
    }
}
