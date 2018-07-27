package cn.edu360.JavaSpark.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavalambdaWC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("JavaLambdaWC");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(args[0]);

        JavaRDD<String> data = lines.flatMap(t -> Arrays.asList(t.split(" ")).iterator());

        JavaPairRDD<String, Integer> withOne = data.mapToPair(t -> new Tuple2<>(t, 1));

        JavaPairRDD<String, Integer> reduce = withOne.reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, String> swaped = reduce.mapToPair(t -> t.swap());

        JavaPairRDD<Integer, String> sorted = swaped.sortByKey();

        JavaPairRDD<String, Integer> result = sorted.mapToPair(t -> t.swap());

        result.saveAsTextFile(args[1]);


    }
}
