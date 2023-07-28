import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ActionMethodsJavaRDD {

    public static void run(JavaSparkContext sparkContext) {
        JavaRDD<Integer> list = sparkContext.parallelize(Arrays.asList(1, 2, 3, 3));

        // reduce - операция над всеми элементами
        System.out.println(list.reduce((x, y) -> x + y));

        // collect
        System.out.println(list.collect());

        // count
        System.out.println(list.count());

        // countByValue
        System.out.println(list.countByValue());

        // take
        System.out.println(list.take(2));

        // top
        System.out.println(list.top(2));

        // takeSample
        System.out.println(list.takeSample(false, 1));


    }

}
