package day02
import org.apache.spark.{SparkConf, SparkContext}

/**aggregateByKey
  *
  * 在每个分区 先按照key分组,聚合
  * 再对分区分组聚合
  * @auther create by jhy
  * @date 2018/4/21 15:01
  */
object AggregateByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("as").setMaster("local")
    val sc = new SparkContext(conf)

    val value = sc.parallelize(List(("cat",2),("kitty",4),("cat",2),("hello",2)))

    //求每个key的和
    //1----------------
    val value1 = value.reduceByKey(_+_)

    //2-----------------
    val value2 = value.aggregateByKey(0)(_+_,_+_)

    //aggregateByKey 和 aggregate不同
    //aggregate的初始值 局部,全局都使用   aggregateByKey只使用局部



    sc.stop()

  }
}
