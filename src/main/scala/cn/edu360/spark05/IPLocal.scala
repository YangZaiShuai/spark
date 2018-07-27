package cn.edu360.spark05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
//最初是的方案
//还会有优化方案


object IPLocal {

  // 定义一个方法，把ip地址转换为10进制
  def ip2Long(ip:String):Long={
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i<-0 until fragments.length){
      ipNum=fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //查找索引
  def search(ipNum:Long,ipRules:Array[(Long,Long,String)]):Int={
    var start = 0
    var end = ipRules.length-1
    while(start<=end){
      val middle = (start+end)/2
      if(ipNum>=ipRules(middle)._1 && ipNum<=ipRules(middle)._2){
        return middle
      }else if(ipNum<ipRules(middle)._1) {
        end = middle - 1
      }else{
        start=middle+1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
   // val ipNum = ip2Long("222.173.110.217")

    //读取ip文件，有ip和位置的映射
    val lines: Iterator[String] = Source.fromFile("data/ip.txt").getLines()
    //生成ip和位置的映射关系，转会换成数组，存到内存中
    val ipRules = lines.map({
      t =>
        val rules = t.split("[|]")
        val start = rules(2).toLong
        val end = rules(3).toLong
        val province = rules(6)
        (start, end, province)
    }).toArray

    //读取文件，判断ip
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("IPlocal")
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile("data/access.log")

    val tpData= data.map({
      t =>
        //获取ip
        val line = t.split("[|]")
        val ip = line(1)
        val ipNum = ip2Long(ip)
        //搜索
        val index = search(ipNum,ipRules)
        if (index!= -1){
          (ipRules(index)._3,1)
        }else{
          ("unKnow",1)
        }
    })

    val result = tpData.reduceByKey(_+_)
    //result.foreach(println)

    //写入数据库     不可以用map map是transforma,不执行   得用foreach
    //result.map()
    result.foreach({
      t=>
        var conn:Connection =null
        var pstm:PreparedStatement =null
        try{
          //连接数据库
        //获取链接
        val url = "jdbc:mysql://localhost:3306/my1?charactorEncoding=utf-8"
        conn = DriverManager.getConnection(url,"root","123456")
        val sql = "insert into access_log values(?,?)"
        pstm = conn.prepareStatement(sql)
        //赋值
        pstm.setString(1,t._1)
        pstm.setInt(2,t._2)
          pstm.execute()
        }catch {
          case e:Exception => "出现异常"
        }finally {
          if(pstm!=null) pstm.close()
          if(conn!=null) conn.close()
        }

    })

    sc.stop()

    /** 虽然这里可以直接写到mysql中，但是实际工作中，有很多技术选型
      *  大数据分析的从  再存储的时候，数据量相比之前，   会很小      mysql集群
      *  离线的数据处理
      *  1，直接写入到mysql中
      *  2，写入到hdfs，或者hbase   ---》mysql中
      *  3，写入到其他的列式存储的数据库   mongodb等
      *
      *  实时的业务流程：
      *  写入到redis中   hbase
      *  直接写入到mysql中
      *  技术： flume  + kafka  + mysql
      *  同样的业务需求，有很多种不同的技术选型方案， 某一类的技术，和业务处理，
      */

  }

}
