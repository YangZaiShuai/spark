/*
package day03.最受欢迎老师

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 老师  每个学科TopN   分组TopN
 *
  * @auther create by jhy
  * @date 2018/4/21 16:26
  */
object GroupFavTeacher {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("as").setMaster("local")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val res: RDD[(String, String)] = lines.map(line =>{
      //获取老师
      val lastIndex = line.lastIndexOf("/") + 1
      val tName = line.substring(lastIndex)
      // 获取学科
      val urldata: String = line.substring(0, lastIndex)
      val url: URL = new URL(urldata)
      val host = url.getHost()
      // val split: Array[String] = host.split("\\.")
      val split: Array[String] = host.split("[.]")
      val subName = split(0)
      (subName, tName)
    })

    val TeacherAndOne = res.map(x=>((x._1,x._2),1))

    val reduced = TeacherAndOne.reduceByKey(_+_)

    //分组排序
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1,3)
   // val grouped2: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1,new HashPartitioner(2),true,null)

    grouped.


    reduced.sortBy(- _._2).take(4).foreach(println)

    sc.stop()

  }
}
*/
