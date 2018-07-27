package cn.edu360.sparkStreaming_01

/*
* 累计  所有批次
* */

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object SparkStreaming_Demo2 {


  val updateFunc = (it:Iterator[(String,Seq[Int],Option[Int])]) =>{
    it.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
  }



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    ssc.checkpoint("data/.chkp")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hdp-01",8888)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = words.map((_,1))

    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    reduced.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
