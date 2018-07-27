package cn.edu360.sparkStreaming_02

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by zx on 2017/7/31.
  *
  * SparkStreaming个Kafka0.8直连方式的整合，使用的是Kafka底层高效的API
  *
  *
  * DStream是在哪一端创建的 ：Driver
  * ZKClient是在哪一端创建的 ：Driver
  * 读取历史偏移量是在哪一端：Driver
  * foreachRDD在哪一端调用的 ：Driver
  * RDD调用Transformation和Action是在哪一端 ：Driver
  * 更新偏移量是在哪一端更新的 ：Driver
  *
  */
object KafkaDirectAdv {

  def main(args: Array[String]): Unit = {

    //指定组名
    val group = "g002"
    //创建SparkConf
    val conf = new SparkConf().setAppName("KafkaDirectAdv").setMaster("local[2]")
    //创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(conf, Duration(5000))
    //指定消费的 topic 名字
    val topic = "wc"
    //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    val brokerList = "node-1:9092,node-2:9092,node-3:9092"


    //指定zk的地址，后期更新消费的偏移量时使用(保存偏移量)
    val zkQuorum = "node-1:2181,node-2:2181,node-3:2181"
    //创建 stream 时使用的 topic 名字集合
    val topics: Set[String] = Set(topic)

    //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //获取 zookeeper 中的路径 "/g001/offsets/wc/"  /g002/offsets/wc/
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //准备kafka的参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group, //指定消费者组的ID
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString //从最开始进行消费
    )

    //zookeeper 的host 和 ip，创建一个 client，跟zk进行通信，查询和跟新偏移量
    val zkClient = new ZkClient(zkQuorum)
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    //使用直连方式一个Task对应一个分区
    //"/g001/offsets/wc/{0,1,2}"

    // /g001/offsets/wc/0/10001"
    // /g001/offsets/wc/1/30001"
    // /g001/offsets/wc/2/10001"

    //zkTopicPath  -> /g001/offsets/wordcount/
    //计算这个目录下游几个分区保存了偏移量
    val children = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[(String, String)] = null

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
    if (children > 0) {
      for (i <- 0 until children) {
        // /g001/offsets/wc/0/    10001
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // wc/0
        val tp = TopicAndPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        // wc/0 -> 10001    //wc/1 -> 30001
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      //通过KafkaUtils创建直连的DStream（安装前面计算好了的偏移量继续消费数据）
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      //如果未保存，根据 kafkaParam 的配置使用最新(largest)或者最旧的（smallest） offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    //kafka直连的DStream
    //依次拿出RDD
    kafkaStream.foreachRDD(rdd => {
      //本质上这个RDD是KafkaRDD，将其转换成HasOffsetRange,然后获取偏移量
      //RDD有多个分区，每一分区都对应一个偏移量
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //然后对该RDD进行处理，对某个批次的RDD进行处理
      val lines: RDD[String] = rdd.map(_._2)

      //foreachPartition是一个Action
      lines.foreachPartition(part => {
        //这里面的大代码是写Executor中的Task内执行的
        part.foreach(x => println(x))
      })

      //偏移量是在Driver端
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        //将该 partition 的 offset 保存到 zookeeper
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }

    })



    ssc.start()
    ssc.awaitTermination()

  }






}
