package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @auther create by jhy
 * @date 2018/4/19 0:20
 */
public class JavaWordCountLamda {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lamdaWC").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile("data/word");

        JavaRDD<String> data = lines.flatMap(t -> Arrays.asList(t.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordAndOne = data.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> res = wordAndOne.reduceByKey(((v1, v2) -> v1 + v2));

        res.foreach(str -> System.out.println(str));

/*        JavaRDD<String> flatMap = words.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> WordAndOne = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> res = WordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        res.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1+","+tuple2._2);
            }
        });*/

        jsc.stop();
    }
}
