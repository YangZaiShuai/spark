/*
package cn.edu360.sparkSql_01

import cn.edu360.sparkSql_01.DataSetDemo.getClass
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetWC {
  def main(args: Array[String]): Unit = {
    //创建sparksql对象
    val session: SparkSession = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()   //获取构建器

    import session.implicits._
    //获取sparkcontext对象
    //1第一种方式 这样获取,就和之前一样了-------1-----1------1-----1----------------------
    val sc = session.sparkContext

    val data: Dataset[String] = session.read.textFile("data/wc.txt")

    val words: Dataset[String] = data.flatMap(t=>t.split(" "))
    //自带了schema
    //words.printSchema()
    //还可以自定义schema信息


    //如果复杂,最好自己指定schema信息   这下面的value是自动映射的
   // words.toDF("")  这么指定
/*
    words.createTempView("wc")
    session.sql("select value,count(*) as counts from wc group by value order by counts desc")
   // session.sql("select value,count(*) as counts from wc group by value")
      .show()

    */


    //dsl   不用注册临时表
    //words.select("value").groupBy("value").count().show()
    val select = words.select("value")
    val grouped = select.groupBy("value")
//    grouped.count()
//      .show()

    import org.apache.spark.sql.functions._
    //agg  聚合方法      指定列名  不指定可以用count(n)
    val agg: DataFrame = grouped.agg(count("*") as "cts")
    //.orderBy("count(1)")  默认升序
      //降序  将字段名用$ 符号引用
    //重命名列
    val agg2 = agg.withColumnRenamed("value","word")

    //agg.orderBy($"cts" desc)
    .orderBy($"cts" desc)

   // agg.show()
    agg2.show()


  }

}
*/
