package cn.edu360.spark05.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//时间 id 关键字 .......
//pv 总的访问次数 uv    根据id去重的访问次数         小时为单位
//统计 小时 url 用户id
object PVUVDemo2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("data/data.utf8.10000")

    //小时 id
    val data= file.map({
      line =>
        val splits = line.split("\t")
        val hour = splits(0).split(":")(0)
        val id = splits(1)
        val url = splits(4)
        val host = url.split("/")
        (hour,host(0), id)
    })

    val hourwithOne = data.map(t=>((t._1,t._2),1))
    val pv = hourwithOne.reduceByKey(_+_)
    pv.foreach(println)

    //uv要去重  每小时的id
    val uv = data.distinct().map(t=>((t._1,t._2),1)).reduceByKey(_+_)


    val join: RDD[((String, String), (Int, Int))] = pv.join(uv)

    join.map(t=>(t._1._1,t._1._2,t._2._1,t._2._2))
        .coalesce(1)
      .sortBy(t=>(t._1,-t._3))
    .foreach(println)

  }
}
