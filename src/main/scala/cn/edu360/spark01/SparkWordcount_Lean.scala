package cn.edu360.spark01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SparkWordcount_Lean {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkWordcount")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0),4)

    val split: RDD[String] = line.flatMap(_.split(" "))

    val mapAndOne: RDD[(String, Int)] = split.map((_,1))

    //将key与随机数组合,先进行一次map
    val value = mapAndOne.map({
      t =>
        val word = t._1
        val random = Random.nextInt(5)
        (word +"_"+ random, 1)
    }).reduceByKey(_ + _,4)

    val value1 = value.map({
      t =>
        val word = t._1
        val count = t._2
        val newWord = word.split("_")(0)
        (newWord, count)
    }).reduceByKey(_+_,4)

    val sort = value1.sortBy(_._2)

    sort.saveAsTextFile(args(1))
    println(sort.collect().toBuffer)

  }
}
