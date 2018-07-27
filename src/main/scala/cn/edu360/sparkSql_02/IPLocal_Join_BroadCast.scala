package cn.edu360.sparkSql_02

import cn.edu360.spark05.IPUtils
import org.apache.spark.sql.{Dataset, SparkSession}

//根据ip求归属地
object IPLocal_Join_BroadCast {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    import session.implicits._

    //关联  两张表
    //person 个人信息  name age fv address
    //归属地   address addname

    //需要创建俩dataset
    val ips = session.read.textFile("data/ip.txt")
    val ipRuleDF = ips.map({
      t =>
        val splits = t.split("[|]")
        val start = splits(2).toLong
        val end = splits(3).toLong
        val province = splits(6)
        (start, end, province)
    })
      //不再toDF
      //.toDF("start", "end", "province")
   // collect  之后，就成了本地的数组
    val ipRuleColl = ipRuleDF.collect()
    val ipRuleBC = session.sparkContext.broadcast(ipRuleColl)


    val logs = session.read.textFile("data/access.log")
    // val ipNumDF = logs.map({
     val ipNumDF= logs.map({
       t =>
         val splits = t.split("[|]")
         val ip = splits(1)
         IPUtils.ip2Long(ip)
     })
      .toDF("num")

    //现在只有一张表
    ipNumDF.createTempView("access")
    //获取ip规则库的数据
    val ipRules:Array[(Long,Long,String)] = ipRuleBC.value

    //这么查询  怎么样把ip转换成结果数据(怎么去iprules查询,返回归属地)
    /*
    * 自定义函数
    * 理解：有外部的数据源可以使用，但是自定义函数（定义了业务逻辑）
      * 把ipnum作为函数的输入参数，然后把归属地（省份） 作为返回值
    * */
   // session.sql("select ipnum from access ")

    //自定义函数 ip2Province（）
    session.udf.register("ip2Province",(ipnum:Long)=>{
      ipnum
      val index = IPUtils.search(ipnum,ipRules)
      var province ="unknown"
      if (index != -1){
        province=ipRules(index)._3
      }
      province
    })

    session.sql("select ip2Province(num) as province,count(*) sums from access group by province order by sums desc")
    .show()

    session.close()
  }
}
