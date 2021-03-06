package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Cache
 * Spark非常重要的一个功能特性就是可以将RDD持久化在内存中。
 * 当对RDD执行持久化操作时，
 * 每个节点都会将自己操作的RDD的partition持久化到内存中，
 * 并且在之后对该RDD的反复使用中，
 * 直接使用内存缓存的partition。
 * 这样的话，对于针对一个RDD反复执行多个操作的场景，
 * 就只要对RDD计算一次即可，后面直接使用该RDD，
 * 而不需要反复计算多次该RDD。
 *
 * 持久化一个RDD，只要调用其cache()或者persist()方法即可。
 * 在该RDD第一次被计算出来时，就会直接缓存在每个节点中。
 * 而且Spark的持久化机制还是自动容错的，
 * 如果持久化的RDD的任何partition丢失了，
 * 那么Spark会自动通过其源RDD，
 * 使用transformation操作重新计算该partition。
 *
 * cache()和persist()的区别在于，
 * cache()是persist()的一种简化方式，
 * cache()的底层就是调用的persist()的无参版本，
 * 同时就是调用persist(MEMORY_ONLY)，
 * 将数据持久化到内存中。
 * 如果需要从内存中清楚缓存，那么可以使用unpersist()方法。
 *
 * RDD持久化是可以手动选择不同的策略的。
 * 比如可以将RDD持久化在内存中、持久化到磁盘上、使用序列化的方式持久化，
 * 多持久化的数据进行多路复用。
 * 只要在调用persist()时传入对应的StorageLevel即可。
 * MEMORY_ONLY
 * 以非序列化的Java对象的方式持久化在JVM内存中。如果内存无法完全存储RDD所有的partition，那么那些没有持久化的partition就会在下一次需要使用它的时候，重新被计算
 *MEMORY_AND_DISK
 * 同上，但是当某些partition无法存储在内存中时，会持久化到磁盘中。下次需要使用这些partition时，需要从磁盘上读取。
 * MEMORY_ONLY_SER
 * 同MEMORY_ONLY，但是会使用Java序列化方式，将Java对象序列化后进行持久化。可以减少内存开销，但是需要进行反序列化，因此会加大CPU开销。
 * MEMORY_AND_DSK_SER
 * 同MEMORY_AND_DSK。但是使用序列化方式持久化Java对象。
 * DISK_ONLY
 * 使用非序列化Java对象的方式持久化，完全存储到磁盘上。
 * MEMORY_ONLY_2
 * MEMORY_AND_DISK_2
 * 如果是尾部加了2的持久化级别，表示会将持久化数据复用一份，保存到其他节点，从而在数据丢失时，不需要再次计算，只需要使用备份数据即可。

 Spark提供的多种持久化级别，主要是为了在CPU和内存消耗之间进行取舍。下面是一些通用的持久化级别的选择建议：

 1、优先使用MEMORY_ONLY，如果可以缓存所有数据的话，那么就使用这种策略。因为纯内存速度最快，而且没有序列化，不需要消耗CPU进行反序列化操作。
 2、如果MEMORY_ONLY策略，无法存储的下所有数据的话，那么使用MEMORY_ONLY_SER，将数据进行序列化进行存储，纯内存操作还是非常快，只是要消耗CPU进行反序列化。
 3、如果需要进行快速的失败恢复，那么就选择带后缀为_2的策略，进行数据的备份，这样在失败时，就不需要重新计算了。
 4、能不使用DISK相关的策略，就不用使用，有的时候，从磁盘读取数据，还不如重新计算一次。


 * @auther create by jhy
 * @date 2018/4/19 16:40
 */
public class _036_Cache_Persist {
    public static void main(String[] args) {

        String name = Thread.currentThread().getStackTrace().getClass().getName();
        SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("data/word.txt").cache();
        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println(endTime-beginTime);

        beginTime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        System.out.println(endTime-beginTime);


        sc.stop();

    }
}
