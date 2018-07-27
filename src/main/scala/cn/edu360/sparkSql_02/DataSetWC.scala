package cn.edu360.sparkSql_02

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetWC {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    import session.implicits._
    val path = "data/wc.txt"

    val lines: Dataset[String] = session.read.textFile(path)

    //lines.printSchema()
    //数据预处理
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    val wordDF = words.toDF("word")

    //sql风格
    //临时视图
/*    wordDF.createTempView("v_words")

    val sql = session.sql("select word,count(*) cts from v_words group by word order by cts")

    sql.show()

    session.close()*/

      import org.apache.spark.sql.functions._
    //dsl风格  1........................
    val select = wordDF.select("word")
      //.groupBy("word").agg(count("*") as "cts")
      //  .orderBy($"cts" desc)
     // .show()
        //.printSchema()
    //dsl风格  2........................
//    select.groupBy("word").count().orderBy("count")
//      .show()

    //统计每组个数
    val grouped: DataFrame = select.groupBy("word").count()
    //统计多少组
    val count: Long = grouped.count()

  }
}
