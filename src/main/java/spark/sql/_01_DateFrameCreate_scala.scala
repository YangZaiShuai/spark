package spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @auther create by jhy
  * @date 2018/4/30 23:44
  */
object _01_DateFrameCreate_scala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val df = sqlc.read.json("data/students.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df.col("name"),df.col("age").plus(1)).show()
    df.select(df.col("name"),df.col("age").leq(2)).show()
    df.filter(df.col("age")>21).show()
    df.groupBy("age").count().show()


  }
}
