package cn.edu360.spark03

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

//自定义分区器

object FavTeacherGroup4 {
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
    val AndOne = data.map((_,1))

    //所有学科
    val AllSubs: Array[String] = data.map(_._1).distinct().collect()

    val by: RDD[((String, String), Int)] = AndOne.reduceByKey(new Mypartitioner(AllSubs),_+_)  //发生了一次shuffle

   // val by = reduce.partitionBy(new Mypartitioner(AllSubs)) //一次shuffle

    val partitions = by.mapPartitions({
      it =>
        it.toList.sortBy( -_._2).take(topK).iterator
    })
    partitions.saveAsTextFile("out5")

  }
}

class Mypartitioner5(sub:Array[String]) extends Partitioner{
  // 定义一个集合，存储学科和分区编号的映射关系   分区编号默认是从0 开始
  val subPart = new mutable.HashMap[String,Int]()
  var i = 0
  for(e <- sub){
    subPart(e)= i
    i +=1
  }

  override def numPartitions = sub.size

  override def getPartition(key: Any) = {
    val keyTrans = key.asInstanceOf[(String,String)]
    val trueKey = keyTrans._1

    subPart(trueKey)
  }
}












