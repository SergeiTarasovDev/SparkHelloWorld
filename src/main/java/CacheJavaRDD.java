import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

public class CacheJavaRDD {

    public static void run(JavaSparkContext sparkContext) {
        JavaRDD<Integer> list = sparkContext.parallelize(Arrays.asList(1, 2, 3));
        list.persist(StorageLevel.MEMORY_AND_DISK());
        list.count();
        list.collect();
        list.unpersist();
    }
}
