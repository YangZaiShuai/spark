package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/**
 *
 * map filter mapToPair flatMap
 *
 * @auther create by jhy
 * @date 2018/4/17 18:30
 */
public class Java_Transformation {
    public static void main(String[] args) {

        //map();
        //groupByKey();
        //reduceByKey();
        //join();
        //cogroup();
        //union();
        //cartesian();
        //mapPartions();
       // mapPartionsWithIndex();
        Aggregate();
    }

    private static void Aggregate(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        Integer res = rdd.aggregate(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(res);
        sc.stop();
    }

    /**
     * cartesian  笛卡尔积
     * 重新分区算子
     * coalesce(5)
     * coalesce(5,true)
     * repartition
     *
     * 收集算子(Action)
     * collect  返回值类型是List
     * collectAsMap  返回值是map  功能和collect函数类似。该函数用于Pair RDD，最终返回Map类型的结果。
     */
    private static void cartesian(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 4, 5));

        JavaPairRDD<Integer, Integer> cartesian = rdd1.cartesian(rdd2);

        JavaPairRDD<Integer, Integer> coalesce = cartesian.coalesce(1);
        JavaPairRDD<Integer, Integer> coalesce2 = cartesian.coalesce(1,true);
        JavaPairRDD<Integer, Integer> repartition = cartesian.repartition(5);


        List<Tuple2<Integer, Integer>> collect = repartition.collect();
        Map<Integer, Integer> collectAsMap = repartition.collectAsMap();

        cartesian.foreach(s-> System.out.println(s._1+","+s._2));
        sc.stop();
    }

    /*
     * union 交集
     * intersection  并集
     */

    private static void union(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(7,5,8,9));

        JavaRDD<Integer> rdd = rdd1.union(rdd2);
        rdd.foreach(s-> System.out.println(s));
        System.out.println("==============");
        JavaRDD<Integer> rdd3 = rdd1.intersection(rdd2);
        rdd3.foreach(s-> System.out.println(s));



        sc.stop();
    }


    /**
     * cogroup
     */
    private static void cogroup(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> studenstList = Arrays.asList(
                new Tuple2<Integer, String>(1, "tom"),
                new Tuple2<Integer, String>(3, "rose"),
                new Tuple2<Integer, String>(2, "jack"));
        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(1, 50),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(3, 70),
                new Tuple2<Integer, Integer>(2, 80));

        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(studenstList);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoresList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupRDD = studentRDD.cogroup(scoreRDD);

        cogroupRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> tup) throws Exception {
                System.out.println(tup._1);
                System.out.println(tup._2._1);
                Iterator<Integer> itr = tup._2._2.iterator();
                while(itr.hasNext()){
                    System.out.println(itr.next());
                }
                System.out.println("==========================");
            }
        });

        sc.close();
    }


    /**
     * join   根据key join
     */
    private static void join(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> studenstList = Arrays.asList(
                new Tuple2<Integer, String>(1, "tom"),
                new Tuple2<Integer, String>(3, "rose"),
                new Tuple2<Integer, String>(2, "jack"));
        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(2, 80));

        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(studenstList);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoresList);


        //内连接   连接上才显示
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = studentRDD.join(scoreRDD);

        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> stu) throws Exception {
                System.out.println(stu._1+"ID");
                System.out.println(stu._2._1+"Name");
                System.out.println(stu._2._2+"Score");
                System.out.println("===========");
            }
        });

        //左外链接   左边显示,右边有就显示Some(值)  没有就是None
        studentRDD.leftOuterJoin(scoreRDD);
        //又外链接
        studentRDD.rightOuterJoin(scoreRDD);
        sc.close();
    }


    /**
     * ReduceByKey   SortBykey
     */

    private static void reduceByKey(){
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

        JavaPairRDD<String, Integer> reduceByKeyRDD = scoreRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<String, Integer> sortByKeyRDD = reduceByKeyRDD.sortByKey();

        sortByKeyRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1+","+tuple2._2);
            }
        });

        sc.close();
    }




    /**
     * groupBykey
     */

    private static void groupByKey(){
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
        //针对scores 对班级分组
        JavaPairRDD<String, Iterable<Integer>> scoreGroupBykeyRDD = scoreRDD.groupByKey();

        scoreGroupBykeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> s) throws Exception {
               // System.out.println(s._1+","+s._2);
                System.out.println(s._1+"班级");
                Iterator<Integer> itr = s._2.iterator();
                while(itr.hasNext()){
                    System.out.println(itr.next());
                }
                System.out.println("==============");
            }
        });

        sc.close();
    }

    /**
     * * map filter mapToPair flatMap
     */
    private static void map(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        List<String> lines = Arrays.asList("hello word","word haha","haha hello");

        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<String> lineRDD = sc.parallelize(lines);

        /**
         * flatMap
         */
        JavaRDD<String> flatMapRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String string) throws Exception {
                return Arrays.asList(string.split(" ")).iterator();
            }
        });
        flatMapRDD.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        /**
         * filter算子
         */
        JavaRDD<Integer> filterRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        filterRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        /**
         * map算子
         */
        JavaRDD<Integer> numberResRDD = numberRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        numberResRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });



        /**
         * mapTopair
         */
        JavaPairRDD<Integer, String> WithOneRDD = numberResRDD.mapToPair(new PairFunction<Integer, Integer, String>() {
            public Tuple2<Integer, String> call(Integer integer) throws Exception {
                return new Tuple2<Integer, String>(integer, "haha");
            }
        });
        WithOneRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            public void call(Tuple2<Integer, String> tuple2) throws Exception {
                System.out.println(tuple2._1+tuple2._2);
            }
        });

        sc.close();
    }


    private static void mapPartions(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        JavaRDD<Integer> res = rdd1.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> v1) throws Exception {

                ArrayList<Integer> list = new ArrayList<>();

                while (v1.hasNext()) {
                    Integer next = v1.next();
                    list.add(next * 2);
                }
                return list.iterator();
            }
        });

        res.foreach(s-> System.out.println(s));


        sc.stop();
    }

    private static void mapPartionsWithIndex(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("haha","xixi","hehe","heihei"),2);

        JavaRDD<String> res = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {

                ArrayList<String> list = new ArrayList<>();
                while (v2.hasNext()) {
                    String next = v2.next();
                    String newString = next + "_" + v1;
                    list.add(newString);
                }

                return list.iterator();
            }
        }, true);

        res.foreach(s-> System.out.println(s));


        sc.stop();
    }

}
