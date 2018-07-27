package cn.edu360.sparkStreaming_02.StreamingKafka10

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object StreamingKafka10Demo {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Milliseconds(5000))

    //从kafka拉取数据  创建DStreaming
    val topics = Set[String]("top2")
    val kafkaParams = Map[String, Object](
      //直接写borker的地址
      "bootstrap.servers" -> "hdp-01:9092,hdp-02:9092,hdp-03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g007",
      "auto.offset.reset" -> "earliest", // lastest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(
      ssc,
      //位置策略
      LocationStrategies.PreferConsistent,
      //消息策略
      ConsumerStrategies.Subscribe(topics,kafkaParams)
    )

    //获取Dstreaming中rdd的偏移量
    kafkaStream.foreachRDD( rdd=>{

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //实时处理
        if(!rdd.isEmpty()){
          //操作RDD
          rdd.foreachPartition({
            part=>
              part.foreach(println)
          })
          //更新偏移量 直接保存在kafka
        }
        //异步更新偏移量
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
