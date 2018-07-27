package cn.edu360.spark05.Sort

import org.apache.spark.{SparkConf, SparkContext}

//最low的排序
object SortDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sort")
    val sc = new SparkContext(conf)

    val data = sc.makeRDD(List("dd 20 99","laoduan 35 96","laozhao 30 96","huige 28 9999"))

    val splitData = data.map({
      t =>
        val split = t.split(" ")
        val name = split(0)
        val age = split(1)
        val fv = split(2)
        (name, age, fv)
    })
    splitData.sortBy(_._2,false).foreach(println)

  }
}
