package cn.edu360.sparkStreaming_01

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Kafka_Wordcount {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val zkQuorum = "hdp-01,hdp-02,hdp-03"
    val groupId="g1"
    val topic =Map[String,Int]("wc"->1)

    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))

    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topic)

    val data2: DStream[String] = data.map(t=>t._2)

    val reduce: DStream[(String, Int)] = data2.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    reduce.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
