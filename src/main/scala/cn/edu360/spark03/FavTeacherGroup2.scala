package cn.edu360.spark03

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacherGroup2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]")
      .setAppName("FavTeacher")
    val sc = new SparkContext(conf)
    val topK=2

    //http://bigdata.edu360.cn/laozhang
    val lines: RDD[String] = sc.textFile("teacher2.log")
    val data = lines.map({
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

        (subName, tName)
    })

    val subjects = data.map(_._1).distinct().collect()

    for (sub<-subjects){
      val bigdata = data.filter(_._1==sub)

      val AndOne = bigdata.map((_,1))
      val key = AndOne.reduceByKey(_+_)

      key.sortBy(_._2,false).take(topK).foreach(println)

    }




  }
}
