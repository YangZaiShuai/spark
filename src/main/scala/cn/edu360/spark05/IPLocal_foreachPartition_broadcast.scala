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

object IPLocal_foreachPartition_broadcast {

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

    //广播出去
    val ipRuleBC: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)

    val tpData= data.map({
      t =>
        //获取ip
        val line = t.split("[|]")
        val ip = line(1)
        val ipNum = ip2Long(ip)
        //搜索
        //val index = search(ipNum,ipRules)
        val ipRuleBCFromBC: Array[(Long, Long, String)] = ipRuleBC.value
        val index = search(ipNum,ipRuleBCFromBC)
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
      it=>
        //foreachPartition操作分区  是迭代器

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
          it.foreach({
            t=>
              pstm.setString(1,t._1)
              pstm.setInt(2,t._2)
              pstm.execute()
          })
        }catch {
          case e:Exception => "出现异常"
        }finally {
          if(pstm!=null) pstm.close()
          if(conn!=null) conn.close()
        }

    })


    sc.stop()



  }

}
