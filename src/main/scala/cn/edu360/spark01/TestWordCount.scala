package cn.edu360.spark01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestWordCount {
  def main(args: Array[String]): Unit = {

  val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("hahaha")
  val sc = new SparkContext(conf)

  val lines: RDD[String] = sc.textFile("wc.txt")
    val split: RDD[String] = lines.flatMap(_.split(" "))
    val AndOne: RDD[(String, Int)] = split.map((_,1))
    val reduced: RDD[(String, Int)] = AndOne.reduceByKey(_+_)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2)

  //println(sorted.collect().toBuffer)

    sorted.collect().foreach(println)
  }
}