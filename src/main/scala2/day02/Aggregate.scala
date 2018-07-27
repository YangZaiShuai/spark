package day02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/4/21 14:39
  */
object Aggregate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sss").setMaster("local")
    val sc = new SparkContext(conf)

    val value1 = sc.parallelize(List(1,2,3,4),2)

    val i1 = value1.aggregate(0)(_+_,_+_)   //(初始值)(每个分区的函数,分区之间的函数)
    val i2 = value1.aggregate(0)(Math.max(_,_),_+_)
    println(i1)

    val value2 = sc.parallelize(List("a","b","c","d"),2)
    val str = value2.aggregate("")(_+_,_+_)
    println(str)  //结果是  abcd   cdab 因为两个task不知道那个task先返回

    val str1 = value2.aggregate("|")(_+_,_+_)
    println(str1)  //结果 ||ab|cd  ||cd|ab


    sc.stop()

  }
}
