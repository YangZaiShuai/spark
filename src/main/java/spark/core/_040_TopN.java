package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**取最大的前三个数字
  * @auther create by jhy
  * @date 2018/4/19 17:38
  */
public class _040_TopN {
  public static void main(String[] args) {

    String name = Thread.currentThread().getStackTrace().getClass().getName();
    SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> num = sc.textFile("data/num.txt");
    JavaPairRDD<Integer, String> pairRDD = num.mapToPair(new PairFunction<String, Integer, String>() {
      @Override
      public Tuple2<Integer, String> call(String s) throws Exception {
        return new Tuple2<>(Integer.parseInt(s), s);
      }
    });

    JavaPairRDD<Integer, String> res = pairRDD.sortByKey(false);

    JavaRDD<Integer> Sortresult = res.map(new Function<Tuple2<Integer, String>, Integer>() {
      @Override
      public Integer call(Tuple2<Integer, String> v1) throws Exception {
        return v1._1;
      }
    });

    List<Integer> result = Sortresult.take(3);

    for(Integer i : result){
      System.out.println(i);
    }

    sc.stop();

  }
}
