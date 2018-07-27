package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


/**spark sql 内置函数  导入function
  * @auther create by jhy
  * @date 2018/5/2 14:48
  */
object _06_SparkSql_Hanshu {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
   val userAccessLog =
     Array("2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-01,1122",
      "2015-10-01,1123")

    val userAccessLogRDD: RDD[String] = sc.parallelize(userAccessLog,5)
    val userAccessLogRowRDD: RDD[Row] = userAccessLogRDD.map(log => {
      val strings = log.split(",")
      Row(strings(0), strings(1).toInt)
    })

    val structType = StructType(Array(StructField("date", StringType, true),
      StructField("userId", IntegerType, true)))

    val userAccLogDF = sqlc.createDataFrame(userAccessLogRowRDD,structType)
    //uv 去重后的访问总数

    userAccLogDF.groupBy("date")
                .agg('date,countDistinct('userId))
                 .rdd.map(row=>Row(row.getAs(0),row.getAs(2)))
                .foreach(println)



  }
}
