package cn.edu360.spark05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

//优化方案  ：
// 1、将foreach换成foreachParation    因为不能用map  所以也不能mapParation
//2、再将iprulers 广播出去  免得每个Excutor工作的时候，都得通过网络去Driver查询
//       broadcast 把Driver的应用数据发送到Excutor  在运行的时候，直接在Excutor 读取
//3\ 在hdfs读取ip规则库
//4、提取方法

object IPLocal_foreachPartition_broadcast_hdfs_util{

  def main(args: Array[String]): Unit = {
    //读取文件，判断ip
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("IPlocal")
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("data/access.log")


    //读取ip文件，有ip和位置的映射
    //从hdfs读取ip规则库
    val lines = sc.textFile("data/ip.txt")
    val ipRulesRDD = lines.map({
      t =>
        val split = t.split("[|]")
        val start = split(2).toLong
        val end = split(3).toLong
        val province = split(6)
        (start, end, province)
    })
    val ipRules = ipRulesRDD.collect()

    //广播出去
    val ipRuleBC: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)

    val tpData: RDD[(String, Int)] = data.map({
      t =>
        //获取ip
        val line = t.split("[|]")
        val ip = line(1)
        val ipNum = IPUtils.ip2Long(ip)
        //搜索
        //val index = search(ipNum,ipRules)
        val ipRuleBCFromBC: Array[(Long, Long, String)] = ipRuleBC.value
        val index = IPUtils.search(ipNum,ipRuleBCFromBC)
        if (index!= -1){
          //val province = ipRules(index)._3
          val province = ipRuleBCFromBC(index)._3
          (province,1)
        }else{
          ("unKnow",1)
        }
    })

    val result = tpData.reduceByKey(_+_)
    //result.foreach(println)

    //写入数据库     不可以用map map是transforma,不执行   得用foreach
    //result.map()
    //foreach 是操作每一个元素N个  操作数据库的操作也得进行N次，效率太低
    // foreachParatition  操作分区  效率高，按分区来操作，rdd中的每一个分区，
    // 一个分区，去获取一个conn，该连接作用于整个rdd分区中的所有数据，然后执行操作，然后关闭连

    result.foreachPartition({
        IPUtils.Data2Mysql
    })


    sc.stop()



  }


}
