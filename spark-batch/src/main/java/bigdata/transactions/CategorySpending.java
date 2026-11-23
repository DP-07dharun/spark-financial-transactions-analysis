package bigdata.transactions;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/** Task 1 - Total transaction amount per category. */
public final class CategorySpending {

    public static void main(String[] args) throws Exception {
        String input = Transactions.inputPath(args);
        String output = Transactions.outputPath(args, "category_spending");

        try (JavaSparkContext sc = Transactions.context("Task 1 - CategorySpending")) {
            JavaPairRDD<String, Double> totals = Transactions.load(sc, input)
                    .mapToPair(t -> new Tuple2<>(t.category, t.amount)) // narrow
                    .reduceByKey(Double::sum)                            // wide: shuffle by category
                    .mapToPair(Tuple2::swap)                             // narrow
                    .sortByKey(false)                                    // wide: range shuffle
                    .mapToPair(Tuple2::swap);

            Transactions.deleteIfExists(sc, output);
            totals.map(t -> t._1 + "," + String.format("%.2f", t._2)).coalesce(1).saveAsTextFile(output);

            List<Tuple2<String, Double>> rows = totals.collect();
            double grand = rows.stream().mapToDouble(Tuple2::_2).sum();
            Transactions.banner("Task 1 - Total transaction amount per category");
            for (Tuple2<String, Double> r : rows) {
                System.out.printf("%-15s %12.2f EUR  %5.1f%%%n", r._1, r._2, 100 * r._2 / grand);
            }
            System.out.printf("%-15s %12.2f EUR%n", "TOTAL", grand);
            System.out.println("Saved to " + output);
        }
    }
}
