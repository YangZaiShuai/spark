package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**Scala版  使用反射 RDD->DataFrame 导入隐式转换
  * @auther create by jhy
  * @date 2018/5/1 17:27
  */
object _02_RDD2DataFrameReflect_Scala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    import sqlc.implicits._

    val students: RDD[Student] = sc.textFile("data/students.txt",1)
      .map(line=>line.split(","))
      .map(t=>Student(t(0).toInt,t(1),t(2).toInt))

    val StudentDF: DataFrame = students.toDF()
    StudentDF.registerTempTable("tableA")

    val teenegerDF = sqlc.sql("select * from tableA")
    teenegerDF.show()

    //DataFrame转换为rdd
    val teenegerRDD = teenegerDF.rdd
    teenegerRDD.map( row => Student(row(0).toString.toInt,row(1).toString,row(2).toString.toInt))
        .foreach(stu => println(stu.age+""+stu.id+""+stu.name))

    teenegerRDD.map( row => Student(row.getAs("id"),row.getAs("name"),row.getAs("age")))
      .foreach(stu => println(stu.age+""+stu.id+""+stu.name))


    teenegerRDD.map(row=>{
      val map = row.getValuesMap(Array("id","name","age"))
      Student(map("id").toString.toInt,map("name").toString,map("age").toString.toInt)
    }).foreach(stu => println(stu.age+""+stu.id+""+stu.name))

  }

  case class Student(id:Int,name:String,age:Int)
}
