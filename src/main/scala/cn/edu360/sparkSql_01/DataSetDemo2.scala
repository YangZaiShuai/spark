package cn.edu360.sparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetDemo2 {
  def main(args: Array[String]): Unit = {

    //创建sparksql对象
    val session: SparkSession = SparkSession.builder()
        .appName(getClass.getName)
        .master("local")
        .getOrCreate()   //获取构建器

    val file: Dataset[String] = session.read.textFile("data/person.txt")

/*    封装成对象     df
      val personRDD = file.map({
      t =>
        val splits = t.split(" ")
        val name = splits(0)
        val age = splits(1).toInt
        val fv = splits(2).toInt
      //Person(name,age,fv)
    })
    val personDF = personRDD.toDF()*/

    import session.implicits._
    val dataset: Dataset[(String, Int, Int)] = file.map({
      t =>
        val splits = t.split(" ")
        val name = splits(0)
        val age = splits(1).toInt
        val fv = splits(2).toInt
        (name, age, fv)
    })                         //这个会自动生成schema信息
    dataset.printSchema()

    //val persondf = dataset .toDF()
    //persondf.printSchema()

    //临时表
// 过时的   dataset.registerTempTable()
//    dataset.createTempView("p")
//    session.sql("select * from p where _2 > 22").show()

    //临时表,自定义schema
    val persondataf = dataset.toDF("name","age","fvv")
    persondataf.createTempView("p2")
    session.sql("select * from p2 where fvv <98")   .show()

  }
}
