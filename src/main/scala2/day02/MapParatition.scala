package day02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/4/21 14:27
  */
object MapParatition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sss").setMaster("local")
    val sc = new SparkContext(conf)

    val value = sc.parallelize(List("haha","xixi","hehe"),2)

    val func = (index:Int,it:Iterator[String]) => {
      //拿到一个分区,在去取分区的数据
      it.map(x=> s"part:$index,ele:$x")
    }

    value.mapPartitionsWithIndex((index,it)=>{
      //拿到一个分区,在去取分区的数据
      it.map(x=> s"part:$index,ele:$x")
    }).foreach(println)

    println("===========")
    value.mapPartitionsWithIndex(func).foreach(println)


    sc.stop()
  }
}
