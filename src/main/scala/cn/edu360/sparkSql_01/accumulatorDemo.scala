package cn.edu360.sparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object accumulatorDemo {
  def main(args: Array[String]): Unit = {

    val mp = new mutable.HashMap[String,Int]()  //Driver段操作的

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("xx")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))

    rdd.foreach({
      t=>
        mp += t                      //这些事excutor操作的
    })

    //这里打印的还是Driver端的
    mp.foreach(println)
  }
}
