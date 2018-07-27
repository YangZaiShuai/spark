package 二次排序

import org.apache.spark.{SparkConf, SparkContext}

object GroupTop3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("asd")
    val sc = new SparkContext(conf)

    val word = sc.textFile("data/sort.txt")

    word.map(line=>{
      val strings = line.split(",")
      (strings(0),strings(1))
    }).groupByKey().map(line=>(line._1,line._2.toList.sorted.reverse.take(3))).foreach(println)

  }
}
