package spark.core._037_共享变量;

import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;


/**
 * Spark提供的Accumulator，
 * 主要用于多个节点对一个变量进行共享性的操作。
 * Accumulator只提供了累加的功能。
 * 但是确给我们提供了多个task对一个变量并行操作的功能。
 * 但是task只能对Accumulator进行 累加 操作，不能读取它的值。
 * 只有Driver程序可以读取Accumulator的值。

 *
 * @auther create by jhy
 * @date 2018/4/19 17:11
 */
public class AccumulatorDemo {
    public static void main(String[] args) {
        String name = Thread.currentThread().getStackTrace().getClass().getName();
        SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建accumulator变量
        Accumulator<Integer> sum = sc.accumulator(0);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 3, 5, 6);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers, 3);

        numberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
            }
        });

        System.out.println(sum);
        sc.close();
    }
}
