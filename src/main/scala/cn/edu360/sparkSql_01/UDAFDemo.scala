package cn.edu360.sparkSql_01

import java.util.Random

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object UDAFDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("123")
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlc = new SQLContext(sc)

    val frame: DataFrame = sqlc.read.json("data/person.json")

    frame.createTempView("person")
     new AVGSalary

    sqlc.udf.register("avg1",new AVGSalary)


    sqlc.sql("select avg1(salary) from person").show()

    sc.stop()
  }
}
/*
* name     salary
* 求平均工资  需要工资总额  工资个数
* */

class AVGSalary extends UserDefinedAggregateFunction{
  //输入数据类型
  override def inputSchema: StructType = StructType(StructField("salary",LongType)::Nil)

  //中间结果
  override def bufferSchema: StructType = StructType(StructField("sum",LongType)::StructField("count",IntegerType)::Nil)

  //结果类型
  override def dataType: DataType =  DoubleType

  //相同输入是否可能有不同输出
  override def deterministic: Boolean = true

  //初始化每个分区的共享变量
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0
  }

  //每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的运算)  局部聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //获取这一行中的工资，降工资假如sum
    buffer(0)= buffer.getLong(0)+input.getAs[Long](0)
    //工资个数+1
    buffer(1)= buffer.getInt(1)+1
  }
  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)

    buffer1(1)=buffer1.getInt(1)+buffer2.getInt(1)
  }

  //结果
  override def evaluate(buffer: Row): Any = {
    //取出总工资，个数
    //相除
    buffer.getLong(0).toDouble/buffer.getInt(1)
  }


}