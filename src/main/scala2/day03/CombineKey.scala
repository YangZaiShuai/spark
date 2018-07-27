package day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**CombineKey必须制定参数类型
  * @auther create by jhy
  * @date 2018/4/21 15:48
  */
object CombineKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("as").setMaster("local")
    val sc = new SparkContext(conf)

    val value = sc.parallelize(List(("cat",2),("kitty",4),("cat",1),("cat",2),("hello",2),("kitty",4)),2)
    /**
      * 第一个参数:
      */
    val value1: RDD[(String, Int)] = value.combineByKey(x => x, (m:Int, n:Int)=>m+n, (a:Int, b:Int)=>a+b)
   // value1.foreach(println)

    value1.cache()
    value1.persist()

    import org.apache.spark.HashPartitioner
    val value2: RDD[(String, Int)] = value.combineByKey(x => x, (m:Int, n:Int)=>m+n, (a:Int, b:Int)=>a+b,new HashPartitioner(3),true,null)
    value1.foreach(println)


    sc.stop()
  }
}
