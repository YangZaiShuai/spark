
package cn.edu360.sparkSql_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object DataSetDemo {
  def main(args: Array[String]): Unit = {

    //创建sparksql对象
    val session: SparkSession = SparkSession.builder()
        .appName(getClass.getName)
        .master("local")
        .getOrCreate()   //获取构建器

    //获取sparkcontext对象
    //1第一种方式 这样获取,就和之前一样了-------1-----1------1-----1----------------------
    val sc = session.sparkContext

    val data: RDD[String] = sc.textFile("data/person.txt")

    val personRDD = data.map({
      t =>
        val splits = t.split(" ")
        val name = splits(0)
        val age = splits(1).toInt
        val fv = splits(2).toInt
        Person(name, age, fv)
    })

    import session.implicits._
    //需要导入隐式转换
    val personDF:DataFrame = personRDD.toDF()//利用反射推导出schema信息
    //把datafram注册成临时表
    personDF.registerTempTable("p")

    //sql风格
    val sql: DataFrame = session.sql("select * from p ")
    //调用action执行
    // sql.show()

    //sql.write.

    sql.show()

  }
}

