package spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @auther create by jhy
  * @date 2018/5/2 14:26
  */
object _05_ParquetLoadData_scala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val usersDF = sqlc.read.parquet("data/users.parquet")
    usersDF.registerTempTable("usersDF")

    sqlc.sql("select * from usersDF").show()

  }
}
