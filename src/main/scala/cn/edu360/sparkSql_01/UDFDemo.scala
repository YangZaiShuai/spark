package cn.edu360.sparkSql_01

import java.util.Random

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDFDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("123")
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlc = new SQLContext(sc)

    val frame: DataFrame = sqlc.read.csv("data/person.txt")

    val ran1 = new Random()

    sqlc.udf.register("add",(x:String)=>ran1.nextInt(3)+x)

    frame.createTempView("person")
    sqlc.sql("select add(_c0) from person").show()

  }
}
