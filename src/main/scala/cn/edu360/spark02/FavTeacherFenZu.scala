package cn.edu360.spark02

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FavTeacherFenZu {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
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


/*    reduce.groupBy({
      t=>
        //分组
        t._1._1
    })*/

    //分组
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduce.groupBy({
      case ((k, v1), v2) => k
    })
/*
(javaee,CompactBuffer(((javaee,wang),8), ((javaee,xiaoxu),6), ((javaee,laoyang),27), ((javaee,laozhang),5)))
(php,CompactBuffer(((php,san),3), ((php,laoliu),5), ((php,laoli),12)))
(bigdata,CompactBuffer(((bigdata,dingding),7), ((bigdata,laozhang),2), ((bigdata,laozhao),25), ((bigdata,laoduan),10)))
* */

    val values: RDD[(String, List[((String, String), Int)])] = grouped.mapValues({
      it =>
        it.toList.sortBy(_._2).reverse.take(2)
    })
/*
* (javaee,List(((javaee,laoyang),27), ((javaee,wang),8)))
(php,List(((php,laoli),12), ((php,laoliu),5)))
(bigdata,List(((bigdata,laozhao),25), ((bigdata,laoduan),10)))*/

    val result2 = values.map({
      case (subject, v) =>
        (subject, v.map(t => (t._1._2, t._2)))
    })


    result2.collect().foreach(println)



  }
}
