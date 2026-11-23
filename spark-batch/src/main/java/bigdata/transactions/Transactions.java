package bigdata.transactions;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/** Shared helpers: Spark context creation, CSV loading/parsing and output handling. */
public final class Transactions {

    public static final String DEFAULT_INPUT = "hdfs:///project/input/transactions.csv";
    public static final String OUTPUT_ROOT = "hdfs:///project/output/";

    // CSV column indexes
    public static final int USER_ID = 0;
    public static final int TRANSACTION_ID = 1;
    public static final int BANK_ACCOUNT_ID = 2;
    public static final int INSTITUTION_ID = 3;
    public static final int DATE = 4;
    public static final int THIRD_PARTY = 5;
    public static final int CATEGORY = 6;
    public static final int AMOUNT = 7;
    public static final int CITY = 8;
    public static final int PAYMENT_METHOD = 9;
    public static final int CURRENCY = 10;
    public static final int COLUMNS = 11;

    private Transactions() {}

    /** One parsed CSV row. Serializable because Spark ships it between executors. */
    public static final class Txn implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String userId;
        public final String transactionId;
        public final String bankAccountId;
        public final int institutionId;
        public final String date; // yyyy-MM-dd HH:mm:ss
        public final String thirdParty;
        public final String category;
        public final double amount;
        public final String city;

        Txn(String[] f) {
            userId = f[USER_ID];
            transactionId = f[TRANSACTION_ID];
            bankAccountId = f[BANK_ACCOUNT_ID];
            institutionId = Integer.parseInt(f[INSTITUTION_ID]);
            date = f[DATE];
            thirdParty = f[THIRD_PARTY];
            category = f[CATEGORY];
            amount = Double.parseDouble(f[AMOUNT]);
            city = f[CITY];
        }

        /** "yyyy-MM" */
        public String month() {
            return date.substring(0, 7);
        }
    }

    public static JavaSparkContext context(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        return sc;
    }

    public static String inputPath(String[] args) {
        return args.length > 0 ? args[0] : DEFAULT_INPUT;
    }

    public static String outputPath(String[] args, String taskName) {
        return args.length > 1 ? args[1] : OUTPUT_ROOT + taskName;
    }

    /** Raw lines without the header (same steps as the Spark Shell version). */
    public static JavaRDD<String> lines(JavaSparkContext sc, String path) {
        JavaRDD<String> data = sc.textFile(path);
        final String header = data.first();
        return data.filter(line -> !line.equals(header));
    }

    /** Parsed transactions; malformed lines are dropped instead of failing the job. */
    public static JavaRDD<Txn> load(JavaSparkContext sc, String path) {
        return lines(sc, path)
                .map(line -> line.split(",", -1))
                .filter(f -> f.length == COLUMNS && isNumber(f[AMOUNT]) && isNumber(f[INSTITUTION_ID]))
                .map(Txn::new);
    }

    private static boolean isNumber(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /** Spark refuses to overwrite an existing output directory, so remove our previous run. */
    public static void deleteIfExists(JavaSparkContext sc, String output) throws IOException {
        Configuration hadoopConf = sc.hadoopConfiguration();
        Path path = new Path(output);
        FileSystem fs = path.getFileSystem(hadoopConf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public static void banner(String title) {
        System.out.println();
        System.out.println("==================== " + title + " ====================");
    }
}
