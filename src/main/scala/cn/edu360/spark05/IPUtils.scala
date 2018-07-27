package cn.edu360.spark05

import java.sql.{Connection, DriverManager, PreparedStatement}

object IPUtils {
  // 定义一个方法，把ip地址转换为10进制
  def ip2Long(ip:String):Long={
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i<-0 until fragments.length){
      ipNum=fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //方法变函数
  val ip2Long_fun=(ip:String)=>{
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
  //变成函数  ，return不能用  去掉return 会死循环
  //解决办法  定义一个标识
  val flag = false
  val result = -1
  val search_fun=(ipNum:Long,ipRules:Array[(Long,Long,String)])=>{
    var start = 0
    var end = ipRules.length-1
    while(start<=end && !flag){
      val middle = (start+end)/2
      if(ipNum>=ipRules(middle)._1 && ipNum<=ipRules(middle)._2){
        //return middle
        middle
        flag==true
        result==middle
      }else if(ipNum<ipRules(middle)._1) {
        end = middle - 1
      }else{
        start=middle+1
      }
    }
    //判断返回值
      result
  }


  val Data2Mysql=(it:Iterator[(String,Int)]) => {
    //foreachPartition操作分区  是迭代器

    var conn: Connection = null
    var pstm: PreparedStatement = null
    try {
      //连接数据库
      //获取链接
      val url = "jdbc:mysql://localhost:3306/my1?charactorEncoding=utf-8"
      conn = DriverManager.getConnection(url, "root", "123456")
      val sql = "insert into access_log values(?,?)"
      pstm = conn.prepareStatement(sql)
      //赋值
      it.foreach({
        t =>
          pstm.setString(1, t._1)
          pstm.setInt(2, t._2)
          pstm.execute()
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (pstm != null) pstm.close()
      if (conn != null) conn.close()
    }
  }
}
