package cn.edu360.sparkSql_01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object OverFunction extends App {

    val conf = new SparkConf()
    conf.setAppName("123")
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlc = new SQLContext(sc)

    import sqlc.implicits._
    val scoreDF: DataFrame = sc.makeRDD(Array(
      Score("a", 1, 80),
      Score("b", 2, 90),
      Score("c", 3, 70),
      Score("d", 1, 80),
      Score("e", 3, 70),
      Score("f", 2, 80),
      Score("g", 1, 70),
      Score("h", 3, 80),
      Score("i", 2, 90)
    )).toDF("name", "class", "score")
    scoreDF.createTempView("score")

    sqlc.sql(" select * from (select name,class,score,row_number() over(partition by class order by score desc ) rank from score) as tb where rank = 1").show()


}
case class Score(name:String,clazz:Int,score:Int)