package cn.edu360.Test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/3/1 17:42
  */
object test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("test1")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/data.txt")
    val unit = lines.map(_.split(",")).map(t => {
      (s"${t(0)}-${t(1)}", t(2))
    }).sortByKey()
    val value = unit.groupByKey().map(t=>(t._1,t._2.toList.sortBy(_.toInt)/*sortWith(_.toInt>_.toInt)*/))
    value.collect().foreach(println)
    sc.stop()

  }
}

