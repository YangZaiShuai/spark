
package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/3/1 18:50
  */
object wordcount {
  def main(args: Array[String]): Unit = {

/*    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)*/

    //val lines: RDD[String] = sc.textFile("data/word.txt")
   // val lines = Array[String]("haha,xixi,heihei","heihei,heihei,xixi")

    //val data= lines.flatMap(x=>x.split(","))

   // val wordAndOne: RDD[(String, Int)] = data.map(t=>(t,1))

   // val res = wordAndOne.reduceByKey(_+_)

    /*val sort: RDD[(String, Int)] = res.sortBy(_._2)*/

    //sort.coalesce(1).saveAsTextFile("res/res.txt")
   // data.foreach(println)
  }
}

