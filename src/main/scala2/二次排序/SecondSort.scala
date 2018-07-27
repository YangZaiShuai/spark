package 二次排序

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        conf.setMaster("local")
    conf.setAppName("asd")
    val sc = new SparkContext(conf)


    val lines = sc.textFile("data/sort.txt")

    val idAndScore: RDD[(String, Int)] = lines.map(x => {
      val split = x.split(",")
      (split(0), split(1).toInt)
    })

    val groupBy: RDD[(String, Iterable[Int])] = idAndScore.groupByKey()

    groupBy.map(line=>(line._1,line._2.toList.sorted)).sortBy(_._1).foreach(println)

  }
}
