package cn.edu360.sparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/*1.x  版本
* DataFrame  的   case class 反射生成schema信息
* */

object SparksqlDFDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)
    //创建一个sqparksql操作对象
    val sqlc = new SQLContext(sc)
    val data: RDD[String] = sc.textFile("data/person.txt")
    val personRDD: RDD[Person] = data.map({
      t =>
        val splits = t.split(" ")
        val name = splits(0)
        val age = splits(1).toInt
        val fv = splits(2).toInt
        Person(name, age, fv)
    })

    import sqlc.implicits._
    //需要导入隐式转换
    val personDF:DataFrame = personRDD.toDF()//利用反射推导出schema信息
    //两种schema打印方式
   // println(personDF.schema)
    //personDF.printSchema()

    //执行具体的操作
    //两种风格

    //把datafram注册成临时表   //旧语法1.6开始     CreateOrReplaceTemview  2.0开始
    personDF.registerTempTable("p")


    //sql风格
    val sql: DataFrame = sqlc.sql("select * from p where age > 20 and fv >95")
    //调用action执行
   // sql.show()

   import org.apache.spark.sql.functions._
    //DSL风格 domain-special-language  定义特定语言
   // personDF.select("name").show()
    //personDF.select("name","age").show()
    personDF.select(col("name"),col("age"),col("fv"))//.show()
   // personDF.select(personDF.col("age"),personDF.col("name")).show()
   // personDF.select(personDF("age")).show()

      //where条件
      //.where($"age">20)
      //.orderBy($"fv" desc )

      //还可以groupby
      //.groupBy("age").count().show()

      .filter(col("age")>22).show()

  }
}
case class Person(val name:String,val age:Int,val fv:Int)
