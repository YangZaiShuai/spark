package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @auther create by jhy
 * @date 2018/4/19 16:23
 */
public class _033_TransAndAction {
    public static void main(String[] args) {

        /**RDD:两种
         * Spark支持两种RDD操作：transformation和action。
         * transformation操作会针对已有的RDD创建一个新的RDD；
         * 而action则主要是对RDD进行最后的操作，
         * 比如遍历、reduce、保存到文件等，并可以返回结果给Driver程序。

         *例如，map就是一种transformation操作，
         * 它用于将已有RDD的每个元素传入一个自定义的函数，
         * 并获取一个新的元素，然后将所有的新元素组成一个新的RDD。
         * 而reduce就是一种action操作，它用于对RDD中的所有元素进行聚合操作，
         * 并获取一个最终的结果，然后返回给Driver程序。

         transformation的特点就是lazy特性。
         lazy特性指的是，如果一个spark应用中只定义了transformation操作，
         那么即使你执行该应用，这些操作也不会执行。
         也就是说，transformation是不会触发spark程序的执行的，
         它们只是记录了对RDD所做的操作，但是不会自发的执行。
         只有当transformation之后，接着执行了一个action操作，
         那么所有的transformation才会执行。Spark通过这种lazy特性，
         来进行底层的spark应用执行的优化，避免产生过多中间结果。

         action操作执行，会触发一个spark job的运行，
         从而触发这个action之前所有的transformation的执行。
         这是action的特性。

         * */

        //统计每行出现的次数
        String name = Thread.currentThread().getStackTrace().getClass().getName();
        SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("data/word.txt");

        lines.mapToPair(x->new Tuple2<>(x,1))
                .reduceByKey(((v1, v2) -> v1 + v2))
                .foreach(x-> System.out.println(x._1+","+x._2));

//        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<>(s, 1);
//            }
//        });
//        JavaPairRDD<String, Integer> reduceByKeyRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v1;
//            }
//        });
//
//        reduceByKeyRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2._1+","+tuple2._2);
//            }
//        });



        sc.close();

    }
}
