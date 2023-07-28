import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;

public class TransformMethodsJavaRDD {

    public static void run(JavaSparkContext sparkContext) {
        JavaRDD<Integer> rddFromList = sparkContext.parallelize(Arrays.asList(1, 3, 8, 19));

        // map
        JavaRDD<Integer> map = rddFromList.map(x -> x * 2);
        map.foreach(x -> System.out.println(x));

        // filter
        JavaRDD<Integer> filter = map.filter(x -> x != 2);
        filter.foreach(x -> System.out.println(x));

        // flatMap
        JavaRDD<Object> flatMap = filter.flatMap(x -> new ArrayList(Arrays.asList(x, x * 2)).iterator());
        flatMap.foreach(x -> System.out.println(x));

        // distinct
        JavaRDD<Integer> list1 = sparkContext.parallelize(Arrays.asList(1, 1, 1, 3, 8, 3));
        list1.distinct().foreach(x -> System.out.println(x));

        // union
        JavaRDD<Integer> list2 = sparkContext.parallelize(Arrays.asList(3, 4, 8));
        list1.union(list2).foreach(x -> System.out.println(x));

        // intersection
        list1.intersection(list2).foreach(x -> System.out.println(x));

        // subtract
        list1.subtract(list2).foreach(x -> System.out.println(x));
    }
}
