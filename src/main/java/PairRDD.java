import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PairRDD {
    public static void run(JavaSparkContext sparkContext) {
        JavaRDD<String> lines = sparkContext.textFile("src/main/resources/temp.txt");
        System.out.println(lines.count());

    }
}
