package cn.edu360.sparkSql_02.SparkSql_DataSource

import java.util.Properties

import org.apache.spark.sql._
/*
* 获取jdbc数据源 1.session.read.jdbc(url,tname,p)
*                2.session.read.format("jdbc").jdbc(url,tname,p)
*                3.  session.read.format("jdbc").options()
* ①查询mysql数据库
*
*
* ②写数据库
*
*
* 四种保存模式"error"(default) append  overwrite  ignore
* */


object JDBC_DataSource {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()
   // +++++++++++++++①查询mysql数据库++++++++++++++++++++++++++++
    val url = "jdbc:mysql://localhost:3306/my1?characterEncoding=utf-8"
    val tname = "access_log"
    val p = new Properties()
    p.setProperty("user","root")
    p.setProperty("password","123456")
    p.setProperty("Driver","com.mysql.jdbc.Driver")
    //1.---------------------------------------------------
    //val jdbc: DataFrame = session.read.jdbc(url,tname,p)
    val jdbc: Dataset[Row] = session.read.jdbc(url,tname,p)

    //jdbc.printSchema()


    //sql 风格 注册为临时表
    jdbc.createTempView("access_log")
   // session.sql("select * from access_log")   .show()

    //dsl风格
    import session.implicits._
    // jdbc.select($"address").show()

    //2.---------------------------------------------------
    val jdbc2 = session.read.format("jdbc").jdbc(url,tname,p)
    jdbc2.createTempView("access_log2")
   // session.sql("select * from access_log2")   .show()

//3.----------------------------------------------------------------------
    val options: DataFrameReader = session.read.format("jdbc").options(Map(
      "url" -> "jdbc:mysql://localhost:3306/my1?characterEncoding=utf-8",
      "dbtable" -> "access_log",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "123456"
    ))
    //执行load,把数据加载出来 生成dataframe
    val option2 = options.load()
   // option2.select("address","count")   .where($"count">2000)   .show()

    // +++++++++++++++②写mysql数据库++++++++++++++++++++++++++++
    //测试  将上边查到的当做数据写入数据库

    val res: Dataset[Row] = option2.select("address","count").where($"count">2500)
    //写json
      // res.write.json("data/json")
     // val res2 = option2.select("address").where($"count">2000)
    //写txt 仅仅能存一列数据   而且必须是string
    // res2.write.text("data/txt")

    //默认保存模式
    res.write.jdbc(url,"access_log2",p)
//    res.write.mode(SaveMode.Overwrite).jdbc(url,"access_log2",p)
   // res.write.mode(SaveMode.Append).jdbc(url,"access_log2",p)
   // res.write.csv("data/csv")
   // res.write.parquet("data/parquet")

  }
}
