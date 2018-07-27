package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @auther create by jhy
 * @date 2018/4/19 15:41
 */
public class _032ParallelzeCollectiion_ {

    public static void main(String[] args) {

        /**
         * Java获取当前类名的两种方法
         适用于非静态方法：this.getClass().getName()
         适用于静态方法：Thread.currentThread().getStackTrace()[1].getClassName()
         */
        String name = Thread.currentThread().getStackTrace().getClass().getName();
        SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建RDD
        //1----并行化创建   一个分区 一个task
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = sc.parallelize(data,3);
        Integer res = javaRDD.aggregate(0, new Function2<Integer, Integer, Integer>() {
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


        //2---------读取本地文件
        //3---------读取HDFS文件





        sc.stop();

    }
}
