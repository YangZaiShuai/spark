package cn.edu360.spark02

import org.apache.spark.{SparkConf, SparkContext}

object PartitionsWithIndex {
  def main(args: Array[String]): Unit = {

    //定义一个
    val fun=(index:Int,it:Iterator[Int])=>{
      it.map({
        v=>
          "part:".concat(index.toString).concat(",v:").concat(v.toString)
      })
    }

    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("SparkWordcount")
    val sc = new SparkContext(conf)
    val arr = sc.makeRDD(Array(1,2,6,9))

    println(arr.mapPartitionsWithIndex(fun).collect().toBuffer)




  }
}
