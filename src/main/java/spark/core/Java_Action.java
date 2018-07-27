package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.*;

/**
 * @auther create by jhy
 * @date 2018/4/17 22:41
 */
public class Java_Action {
    public static void main(String[] args) {

        //reduce();
        //collect();
       // count();
       // take();
        //saveAsTextFile();
         CountByKey();
    }




    /**
     * CountByKey
     */

    private static void CountByKey(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> scores = Arrays.asList(
                new Tuple2<String, Integer>("class1",90),
                new Tuple2<String, Integer>("class2",50),
                new Tuple2<String, Integer>("class3",60),
                new Tuple2<String, Integer>("class1",70),
                new Tuple2<String, Integer>("class3",80),
                new Tuple2<String, Integer>("class3",20),
                new Tuple2<String, Integer>("class2",40),
                new Tuple2<String, Integer>("class2",90));
        //并行化集合创建RDD
        JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(scores);

        Map<String, Long> countByKey = scoreRDD.countByKey();

        Set<Map.Entry<String, Long>> entrySet = countByKey.entrySet();

        for (Map.Entry<String, Long> cbk : entrySet){
            System.out.println(cbk.getKey()+","+cbk.getValue());
        }

        sc.close();
    }



    /**
     *saveAsTextFile
     */

    private static void saveAsTextFile(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);

        JavaRDD<Integer> mapRDD = numberRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        mapRDD.saveAsTextFile("data/res.txt");


        sc.close();
    }



    /**
     *take
     */

    private static void take(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);

        JavaRDD<Integer> mapRDD = numberRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        List<Integer> takeRDD = mapRDD.take(3);

        for(Integer num : takeRDD){
            System.out.println(num);
        }

        Iterator<Integer> itr = takeRDD.iterator();
        while(itr.hasNext()){
            System.out.println(itr.next());
        }

        sc.close();
    }


    /**
     *count
     */

    private static void count(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);

        JavaRDD<Integer> mapRDD = numberRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        long count = mapRDD.count();

        System.out.println(count);


        sc.close();
    }



    /**
     *collect
     */

    private static void collect(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);

        JavaRDD<Integer> mapRDD = numberRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        List<Integer> collect = mapRDD.collect();

        System.out.println(collect);


        sc.close();
    }


    /**
     *Reduce
     */

    private static void reduce(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);

        Integer reduceRDD = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(reduceRDD);


        sc.close();
    }
}
