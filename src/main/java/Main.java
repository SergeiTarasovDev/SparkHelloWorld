import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkHW");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

//        TransformMethodsJavaRDD.run(sparkContext);
//        ActionMethodsJavaRDD.run(sparkContext);
//        CacheJavaRDD.run(sparkContext);
        PairRDD.run(sparkContext);
    }
}
