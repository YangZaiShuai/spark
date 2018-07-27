package cn.edu360.sparkSql_02

import cn.edu360.spark05.IPUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//根据ip求归属地
object IPLocal_Join {
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
    }).toDF("start", "end", "province")

    val logs = session.read.textFile("data/access.log")
    val ipNumDF = logs.map({
      t =>
        val splits = t.split("[|]")
        val ip = splits(1)
        IPUtils.ip2Long(ip)
    }).toDF("num")

    //闯将两个临时视图
    ipRuleDF.createTempView("ipRules")
    ipNumDF.createTempView("access")


    //join   sql
//    session.sql("select province,count(*) conts from ipRules join access on ( access.num >= ipRules.start and access.num <= ipRules.end ) group by province")
//      .show()

    ipRuleDF.join(ipNumDF)
      .where($"num">=$"start" and $"num"<=$"end" )
        .groupBy($"province")
      .count()
        //重命名,排序
        //1------  .withColumnRenamed("count","cnt").sort($"cnt")
      /*  2------  */.toDF("pro","cnts").sort($"cnts" desc)
      //.show()

    //join的条件中 添加条件
    ipRuleDF.join(ipNumDF,$"num">=$"start" and $"num"<=$"end","inner").groupBy($"province").count()
      .show()

      //between'
      ipRuleDF.join(ipNumDF,$"num" between($"start",$"end"),"right").groupBy($"province").count()
      .show()

  }
}
