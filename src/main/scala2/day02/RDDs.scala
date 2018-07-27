package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/4/21 15:16
  */
object RDDs {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("as").setMaster("local")
    val sc = new SparkContext(conf)

    val value = sc.parallelize(List(("cat",2),("kitty",4),("cat",2),("hello",2)))
    val value2 = sc.parallelize(List(("a",2),("b",4),("d",2),("c",2)))
    val value3 = sc.parallelize(List(("a","2 3"),("b","4 5"),("d","2 4"),("c","2 1")))
    val value4 = sc.parallelize(List("dog","wolf","cat"),2)

    /**
      * reduceByKey  aggregateByKey  FoldBykey  comeByKey
      * 都是先进性局部聚合,再进行全局聚合
      * 他们底层调用的都是 comebineByKeyWithClassTag
      *
      * aggregate Action
      * aggregateByKey  Transformation
      *
      * foreach
      * foreachPatition  Action
      *
      */
    value4.map(x=>(x.length,x))
            .foldByKey("")(_+_).foreach(println)


    //countByKey  每个key出现的次数
    //value.countByKey().foreach(println)
    /**
      * (hello,1)
        (kitty,1)
        (cat,2)
      */

    //过滤范围内  包含边界
   // value2.filterByRange("b","d").foreach(println)
    /**
      * (b,4)
        (d,2)
        (c,2)
      */

   // value3.flatMapValues(_.split(" ")).foreach(println)

    /**
      * (a,2)
        (a,3)
        (b,4)
        (b,5)
        (d,2)
        (d,4)
        (c,2)
        (c,1)
      */



    sc.stop()

  }
}
