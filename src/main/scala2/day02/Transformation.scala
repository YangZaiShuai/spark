package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/4/20 23:12
  */
object Transformation {
  def main(args: Array[String]): Unit = {

      val conf = new SparkConf()
        .setAppName("aaa")
      .setMaster("local")
      val sc = new SparkContext(conf)

      val line = sc.textFile("data/word.txt")

      val word: RDD[String] = line.flatMap(_.split(" "))

      word.cache()

      word.count()
      word.take(1)

      sc.stop()

  }
}
