package bigdata.transactions;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/** Task 4 - Top spending cities. */
public final class TopCities {

    public static void main(String[] args) throws Exception {
        String input = Transactions.inputPath(args);
        String output = Transactions.outputPath(args, "top_cities");

        try (JavaSparkContext sc = Transactions.context("Task 4 - TopCities")) {
            JavaPairRDD<Double, String> ranked = Transactions.load(sc, input)
                    .mapToPair(t -> new Tuple2<>(t.city, t.amount))
                    .reduceByKey(Double::sum)
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false);

            Transactions.deleteIfExists(sc, output);
            ranked.map(t -> t._2 + "," + String.format("%.2f", t._1)).coalesce(1).saveAsTextFile(output);

            List<Tuple2<Double, String>> rows = ranked.take(10);
            Transactions.banner("Task 4 - Top spending cities");
            int rank = 1;
            for (Tuple2<Double, String> r : rows) {
                System.out.printf("%2d. %-12s %12.2f EUR%n", rank++, r._2, r._1);
            }
            System.out.println("Saved to " + output);
        }
    }
}
