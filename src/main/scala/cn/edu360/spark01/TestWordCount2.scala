package cn.edu360.spark01

import org.apache.spark.{SparkConf, SparkContext}

object TestWordCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("hh")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("wc.txt")

    val split = lines.flatMap(_.split(" "))

    val AndOne = split.map((_,1))

  //1---  val reduced = AndOne.reduceByKey(_+_)
    val reduced = AndOne.groupByKey().map({
      t =>
        (t._1, t._2.sum)
    })

    val sorted = reduced.sortBy(_._2)

    sorted.collect().foreach(println)
  }
}
