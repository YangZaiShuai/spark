package day01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/3/17 21:05
  */
object test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("as")
    val sc = new SparkContext(conf)

    val line = sc.textFile("data/word.txt")

    val value = line.flatMap(_.split(" ")).map(line=>(line.trim,""))

    value.groupByKey().keys.collect().foreach(println)


  }
}
