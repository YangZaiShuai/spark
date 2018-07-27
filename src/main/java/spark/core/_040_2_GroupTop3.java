/*
package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

*/
/**
 * @auther create by jhy
 * @date 2018/4/19 17:48
 *//*

public class _040_2_GroupTop3 {
    public static void main(String[] args) {
        String name = Thread.currentThread().getStackTrace().getClass().getName();
        SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("data/score.txt");

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String line) throws Exception {
                        String[] lineSplited = line.split(" ");
                        return new Tuple2<String, Integer>(lineSplited[0],
                                Integer.valueOf(lineSplited[1]));
                    }

                });

        JavaPairRDD<String, Iterable<Integer>> groupedPairs = pairs.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3Score = groupedPairs.mapToPair(

                new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Iterable<Integer>> call(
                            Tuple2<String, Iterable<Integer>> classScores)
                            throws Exception {
                        Integer[] top3 = new Integer[3];

                        String className = classScores._1;
                        Iterator<Integer> scores = classScores._2.iterator();

                        while(scores.hasNext()) {
                            Integer score = scores.next();

                            for(int i = 0; i < 3; i++) {
                                if(top3[i] == null) {
                                    top3[i] = score;
                                    break;
                                } else if(score > top3[i]) {
                                    for(int j = 2; j > i; j--) {
                                        top3[j] = top3[j - 1];
                                    }

                                    top3[i] = score;

                                    break;
                                }
                            }
                        }

                        return new Tuple2<String,
                                Iterable<Integer>>(className, Arrays.asList(top3));
                    }

                });

        top3Score.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> scoreIterator = t._2.iterator();
                while(scoreIterator.hasNext()) {
                    Integer score = scoreIterator.next();
                    System.out.println(score);
                }
                System.out.println("=======================================");
            }

        });

        sc.close();

*/
/*
        JavaPairRDD<String, Integer> pairs = rdd.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String line) throws Exception {
                        String[] lineSplited = line.split(" ");
                        return new Tuple2<String, Integer>(lineSplited[0],
                                Integer.valueOf(lineSplited[1]));
                    }

                });

        JavaPairRDD<String, Iterable<Integer>> group = pairs.groupByKey();

*//*

*/
/*        JavaPairRDD<String, Integer> pair = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> group = pair.groupByKey();*//*
*/
/*


        JavaPairRDD<String, Iterable<Integer>> res = group.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> s) throws Exception {

                Integer[] top3 = new Integer[3];
                String className = s._1;
                Iterable<Integer> classScores = s._2;
                Iterator<Integer> itr = classScores.iterator();
                while (itr.hasNext()) {
                    Integer score = itr.next();
                    for (int i = 0; i < 3; i++) {
                        if (top3[i] == null) {
                            top3[i] = score;
                            break;
                        } else if (score > top3[i]) {
                            for(int j = 2; j > i; j--) {
                                top3[j] = top3[j - 1];
                            }

                            top3[i] = score;

                            break;
                        }
                    }
                }

                return new Tuple2<>(className, Arrays.asList(top3));
            }
        });
        res.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> s) throws Exception {
                String className = s._1;
                Iterator<Integer> itr = s._2.iterator();
                while (itr.hasNext()){
                    System.out.println(className+","+itr.next());
                }
            }
        });
*//*



        sc.stop();
    }
}
*/
