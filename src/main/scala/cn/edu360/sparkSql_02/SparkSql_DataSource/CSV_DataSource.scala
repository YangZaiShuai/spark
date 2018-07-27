package cn.edu360.sparkSql_02.SparkSql_DataSource

import org.apache.spark.sql.SparkSession

object CSV_DataSource {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()
    val csv_DF = session.read.csv("data/test.csv")
    csv_DF.printSchema()

    //查询  sql风格
 /*   val csv_DF2 = csv_DF.toDF("name","age","fv")

    csv_DF2.createTempView("csv")

    session.sql("select * from csv where age >18 ").show()*/
   //查询  dsl
  //  csv_DF.select("_c0").show()

    csv_DF.write.csv("data/csv2")


  }
}
