package cn.edu360.sparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, types}
import org.apache.spark.{SparkConf, SparkContext}

/*
* 不再使用  :DataFrame  的   case class 反射生成schema信息   toDF
*          不需要toDF  不需要封装成class对象了
* 封装成row
* 自定义schema信息
    val personSchema = StructType()
*
* */

object SparksqlDFDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)

    //创建一个sqparksql操作对象
    val sqlc = new SQLContext(sc)

    val data: RDD[String] = sc.textFile("data/person.txt")

    val personRDD: RDD[Row] = data.map({
      t =>
        val splits = t.split(" ")
        val name = splits(0)
        val age = splits(1).toInt
        val fv = splits(2).toInt
       //2-------不再封装 Person(name, age, fv)
        Row(name, age, fv)
    })



    import sqlc.implicits._
    //需要导入隐式转换+
    //2-----------不再toDF
//    val personDF:DataFrame = personRDD.toDF()//利用反射推导出schema信息
    //自定义schema信息
    val personSchema: StructType = StructType(
      List(
        // 指定了一个字段的类型 名称  类型  是否为空
        StructField("name", StringType, true),
        StructField("age", IntegerType) ,
          StructField("fv", IntegerType)
      ))
    personSchema


    // 使用sqlc  Row类型的RDD数据和schema信息创建dataframe
    val personDF = sqlc.createDataFrame(personRDD,personSchema)

    personDF.printSchema()

    //执行具体的操作
    //两种风格

    //把datafram注册成临时表
    personDF.registerTempTable("p")


    //sql风格
    val sql: DataFrame = sqlc.sql("select * from p where age > 20 and fv >95")
    //调用action执行
    sql.show()

  //import org.apache.spark.sql.functions._
    //DSL风格
   // personDF.select("name").show()
    //personDF.select("name","age").show()
   // personDF.select(col("name"),col("age"),col("fv")).show()
   // personDF.select(personDF.col("age"),personDF.col("name")).show()
   // personDF.select(personDF("age")).show()

      //where条件
      //.where($"age">20)
      //.orderBy($"fv" desc )

    //还可以groupby
      //.groupBy("age").count().show()

    //  .filter(col("age")>22).show()

    val selectDF = personDF.select("name","age","fv")
    selectDF.filter($"age">22).show()

    val selectDF2 = personDF.select(personDF("name"),personDF("age"),personDF("fv")+1)
    selectDF2.show()
  }
}


//case class Person(val name:String,val age:Int,val fv:Int)
