package cn.edu360;

import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class wc {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("data/word.txt");
        JavaRDD<String> word = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String str) throws Exception {
                return Arrays.asList(str.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> word2int = word.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        JavaPairRDD<String, Integer> res = word2int.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });
        res.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+" : "+t._2);
            }
        });
    }

}
