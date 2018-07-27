package cn.edu360.sparkSql_02.SparkSql_DataSource
/*
* JSON数据源
* 读
*
* 写
*
* */
import org.apache.spark.sql.{DataFrame, SparkSession}

object Json_DataSource {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()
    val jsDF = session.read.json("data/json/test.json")

   // jsDF.printSchema()

    //查json
    //两种风格
    //dsl
    import session.implicits._
   // jsDF.where($"count">3000).show()

    //sql
    jsDF.createTempView("json")
    //session.sql("select * from json").show()
    val res: DataFrame = session.sql("select * from json")

   // res.write.mode(saveMode = "overwrite").json("data/json2")
    res.write.mode("overwrite").json("data/json2")
    //res.write.mode(saveMode = "append").json("data/json2")
    session.close()

  }
}
