package spark.core._037_共享变量

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/4/19 17:17
  */
object accumulatorDemoScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("FavTeacher")
    val sc = new SparkContext(conf)

    val sum = sc.accumulator(0)

    val array = Array(1,2,3,4,5)
    val numberRDD = sc.parallelize(array)

    val res = numberRDD.foreach(num => sum += num)
    println(sum)

    sc.stop()

  }
}
