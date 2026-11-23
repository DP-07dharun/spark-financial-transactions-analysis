package bigdata.transactions;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/** Task 6 - Monthly spending trends (total, count and average ticket per month). */
public final class MonthlySpendingTrends {

    public static void main(String[] args) throws Exception {
        String input = Transactions.inputPath(args);
        String output = Transactions.outputPath(args, "monthly_trends");

        try (JavaSparkContext sc = Transactions.context("Task 6 - MonthlySpendingTrends")) {
            JavaPairRDD<String, Tuple2<Double, Integer>> monthly = Transactions.load(sc, input)
                    .mapToPair(t -> new Tuple2<>(t.month(), new Tuple2<>(t.amount, 1)))
                    .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                    .sortByKey();

            Transactions.deleteIfExists(sc, output);
            monthly.map(t -> t._1 + "," + String.format("%.2f", t._2._1) + "," + t._2._2)
                    .coalesce(1).saveAsTextFile(output);

            List<Tuple2<String, Tuple2<Double, Integer>>> rows = monthly.collect();
            double max = rows.stream().mapToDouble(r -> r._2._1).max().orElse(1);
            Transactions.banner("Task 6 - Monthly spending trends");
            Double previous = null;
            for (Tuple2<String, Tuple2<Double, Integer>> r : rows) {
                double total = r._2._1;
                String change = previous == null ? "      " : String.format("%+5.1f%%", 100 * (total - previous) / previous);
                System.out.printf("%s %11.2f EUR %4d tx avg %6.2f %s %s%n", r._1, total, r._2._2,
                        total / r._2._2, change, bar((int) Math.round(total / max * 30)));
                previous = total;
            }
            System.out.println("Saved to " + output);
        }
    }

    private static String bar(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append('#');
        return sb.toString();
    }
}
