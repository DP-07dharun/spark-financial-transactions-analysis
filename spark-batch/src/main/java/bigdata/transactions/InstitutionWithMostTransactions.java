package bigdata.transactions;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/** Task 5 - Financial institutions handling the most transactions. */
public final class InstitutionWithMostTransactions {

    public static void main(String[] args) throws Exception {
        String input = Transactions.inputPath(args);
        String output = Transactions.outputPath(args, "institutions");

        try (JavaSparkContext sc = Transactions.context("Task 5 - InstitutionWithMostTransactions")) {
            JavaPairRDD<Integer, Integer> ranked = Transactions.load(sc, input)
                    .mapToPair(t -> new Tuple2<>(t.institutionId, 1))
                    .reduceByKey(Integer::sum)
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false);

            Transactions.deleteIfExists(sc, output);
            ranked.map(t -> t._2 + "," + t._1).coalesce(1).saveAsTextFile(output);

            List<Tuple2<Integer, Integer>> rows = ranked.collect();
            long total = rows.stream().mapToLong(Tuple2::_1).sum();
            Transactions.banner("Task 5 - Financial institutions handling the most transactions");
            for (Tuple2<Integer, Integer> r : rows) {
                System.out.printf("Institution %3d  %5d transactions  %5.1f%%%n", r._2, r._1, 100.0 * r._1 / total);
            }
            Tuple2<Integer, Integer> leader = rows.get(0);
            System.out.printf("Most transactions: institution %d (%d)%n", leader._2, leader._1);
            System.out.println("Saved to " + output);
        }
    }
}
