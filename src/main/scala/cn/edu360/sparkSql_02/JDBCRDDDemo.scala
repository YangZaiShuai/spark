package cn.edu360.sparkSql_02

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*  spark 提供了一个JdbcRDD 用于查询mysql*/
object JDBCRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
    val sc=  new SparkContext(conf)

    /**
      * sc: SparkContext,
    getConnection: () => Connection,   获取连接
    sql: String,    sql语句
    lowerBound: Long, 绑定的第一个参数
    upperBound: Long, 绑定的第二个参数
    numPartitions: Int, 分区个数
    mapRow: (ResultSet) 返回值
      */
    // JdbcRDD
    val getConn = () =>{
      DriverManager.getConnection("jdbc:mysql://localhost:3306/my1?characterEncoding=utf-8","root","123456")
    }

    /**
      *  两个分区  第一个分区   1 2    第二个分区是 3 4 5
      *  grade >= 1 and grade < 2
      *  grade >=3 and grade <5
      */
    val jdbc = new JdbcRDD(
      sc,
      getConn,
      "select * from access_log where count >= ? and count <= ?",
      1000,
      3000,
      2,
      res => {
        val grade = res.getString(1)
        val low = res.getInt(2)
        (grade, low)
      }
    )
    jdbc.collect().foreach(println)
  }

}
