package cn.edu360.spark03

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacherGroup1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]")
      .setAppName("FavTeacher")
    val sc = new SparkContext(conf)
    val topK=2

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
    /**
      * ((javaee,xiaoxu),6)
        ((php,laoliu),1)
        ((bigdata,laozhang),2)
        ((bigdata,laozhao),15)
        ((javaee,laoyang),9)
        ((php,laoli),3)
        ((bigdata,laoduan),6)
      */

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

    val values: RDD[(String, List[(String, Int)])] = grouped.mapValues({
      it =>
        val takeTop = it.toList.sortBy(-_._2).take(topK)
        takeTop.map(t => (t._1._2, t._2))
      /*        takeTop.map({
                case ((_,v),v2)=>(v,v2)
              })*/

    })

    values.foreach(println)



  }
}
