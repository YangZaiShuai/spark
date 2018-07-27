package cn.edu360.sparkSql_01

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object UDAFDemo2 {
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

case class Employee(name:String,salary: Long)
case class Aver(sum:Long,count:Int)

class AVGSalary2 extends Aggregator[Employee,Aver,Double]{
  override def zero = ???

  override def reduce(b: Aver, a: Employee) = ???

  override def merge(b1: Aver, b2: Aver) = ???

  override def finish(reduction: Aver) = ???

  override def bufferEncoder = ???

  override def outputEncoder = ???
}