/*
package cn.edu360.spark02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
* 用groupby 和 groupbykey改造
* */

object SparkWordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkWordcount")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))

    val split: RDD[String] = line.flatMap(_.split(" "))

    //val mapAndOne: RDD[(String, Int)] = split.map((_,1))

    //改成对每个分区操作
    //split.mapPartitions(t=>t.map((_,1)))

    val mapAndOne = split.mapPartitions({
      t =>
        t.map((_, 1))
    })


    //val reduce: RDD[(String, Int)] = mapAndOne.reduceByKey(_+_)
    //val sort = reduce.sortBy(_._2)

   //用groupbykey
/*    val grouped = mapAndOne.groupByKey().map(
      t =>
        (t._1, t._2.sum)
    )
    val sort = grouped.sortBy(_._2)*/
  //用groupby
/*    val grouped = mapAndOne.groupBy(_._1).map(
      t =>
        (t._1, t._2.map(_._2).sum)
    )
    val sort = grouped.sortBy(_._2)*/

    val grouped = mapAndOne.groupBy(_._1).mapValues(
      t =>
        t.map(_._2).sum
    )
    val sort = grouped.sortBy( -_._2)

  sort.saveAsTextFile()
   // sort.saveAsTextFile(args(1))
    println(sort.collect().toBuffer)

  }
}
*/
