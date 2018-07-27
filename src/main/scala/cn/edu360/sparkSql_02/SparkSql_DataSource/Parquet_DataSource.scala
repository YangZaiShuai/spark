package cn.edu360.sparkSql_02.SparkSql_DataSource

import org.apache.spark.sql.SparkSession

object Parquet_DataSource {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()
    //val pDF = session.read.parquet("data/parquet/test.parquet")
    val pDF = session.read.parquet("par/part-00000-57a292ab-59af-4dff-8965-86c6d485050b-c000.snappy.parquet")

    pDF.printSchema()

    //查询 sql
    pDF.createTempView("emp")
    session.sql("select * from emp where count > 3000").show()
    //dsl
   // pDF.select("address","count").show()


    //pDF.write.parquet("par")

  }
}
