package day02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/4/21 0:26
  */
object Actions {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(1,2,3,4,5),2)

    rdd.collect()
    rdd.reduce(_ + _)

    rdd.top(2)    //最大三个
    rdd.take(3)   //取前三个
    rdd.first     //第一个
    rdd.takeOrdered(3)  //排好序,升序  前几个

    sc.stop()

  }
}
