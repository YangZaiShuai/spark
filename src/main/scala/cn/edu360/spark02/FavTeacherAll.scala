package cn.edu360.spark02

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacherAll {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]")
      .setAppName("FavTeacher")
    val sc = new SparkContext(conf)

    //http://bigdata.edu360.cn/laozhang
    val lines: RDD[String] = sc.textFile("teacher2.log")
    val result: RDD[((String, String), Int)] = lines.map({
      line =>
        //获取老师
        val lastIndex = line.lastIndexOf("/") + 1
        val tName = line.substring(lastIndex)
        //  println(tName)
        // 获取学科
        val urldata: String = line.substring(0, lastIndex)
        val url: URL = new URL(urldata)
        val host = url.getHost()
        // println(host)
        val split: Array[String] = host.split("\\.")
        val subName = split(0)

        ((subName, tName), 1)

    })
    val reduce: RDD[((String, String), Int)] = result.reduceByKey(_+_)

    val sorted: RDD[((String, String), Int)] = reduce.sortBy(_._2,false)

    //sorted.collect().foreach(println)

   // println("輸入前幾名老師?")
   // val n = Console.readInt()

    sorted.collect().foreach(println)


  }
}
