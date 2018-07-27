package cn.edu360.spark01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("QWE")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)

    val line = sc.textFile("data/word.txt")

    val splits: RDD[String] = line.flatMap(_.split(" "))

    val reduce: RDD[(String, Int)] = splits.map(x=>(x,1)).reduceByKey(_+_)

    reduce.foreach(println)

  }
}
