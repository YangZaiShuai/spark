package spark.core._037_共享变量;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @auther create by jhy
 * @date 2018/4/19 16:58
 */
public class BroadCastDemo {
    public static void main(String[] args) {

        //默认情况下,算子的函数内,使用了外部的变量,
        //会拷贝到执行这个函数的每一个Task中(每个 worker节点有多个Task)
        //如果这个变量特别大的话,网络传输会特别大,占用大量的内存空间

        //Broadcast
        //在每个节点上只会有一份副本，
        // 而不会为每个task都拷贝一份副本。
        // 因此其最大作用，就是减少变量到各个节点的网络传输消耗，
        // 以及在各个节点上的内存消耗。
        // 此外，spark自己内部也使用了高效的广播算法来减少网络消耗。

        String name = Thread.currentThread().getStackTrace().getClass().getName();
        SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int factor = 3;
        Broadcast<Integer> BroadFactor = sc.broadcast(factor);


        List<Integer> number = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numberRDD = sc.parallelize(number);

        //让集合中每个元素乘 外部的 factor
        JavaRDD<Integer> map = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                //return v1 * factor;
                return v1 * BroadFactor.value();
            }
        });

        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


        sc.close();

    }
}
