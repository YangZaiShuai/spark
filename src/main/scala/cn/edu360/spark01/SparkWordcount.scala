package cn.edu360.spark01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkWordcount")
    conf.set("spark.shuffle.consolidateFile","true")
    val sc = new SparkContext(conf)
    val line: RDD[String] = sc.textFile("data/word.txt")

    val split: RDD[String] = line.flatMap(_.split(" "))

    val mapAndOne: RDD[(String, Int)] = split.map((_,1))

    mapAndOne.coalesce(3)
    mapAndOne.repartition(5)

/*    val reduce: RDD[(String, Int)] = mapAndOne.reduceByKey(_+_)
    reduce.foreach(println)*/
    val value: RDD[(String, Iterable[Int])] = mapAndOne.groupByKey()
    value.map(x=>(x._1,x._2.toList.sum)).foreach(print)



   // val sort = reduce.sortBy(_._2)

    //sort.saveAsTextFile(args(1))
    //println(sort.collect().toBuffer)

  }
}
