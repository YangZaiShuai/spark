package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**Scala版本使用编程方式构造DataFrame
  * @auther create by jhy
  * @date 2018/5/2 1:38
  */
object _03_RDD2DataFrameProgrammatically_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    //1创建RDD,转换为Row类型的
    val studentRDD= sc.textFile("data/students.txt",1)
        .map(line=>{
          val strings = line.split(",")
          Row(strings(0).toInt,strings(1),strings(2).toInt)
        })
    import sqlc.implicits._
    //2构造元数据
    val structType = StructType(Array(
      StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
     )
    )

    //构造DataFrame
    val studentDF = sqlc.createDataFrame(studentRDD,structType)

    studentDF.registerTempTable("tableC")

    sqlc.sql("select * from tableC where age < 18").show()

  }
}
