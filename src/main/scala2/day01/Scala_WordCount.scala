package day01

/**
  * @auther create by jhy
  * @date 2018/3/1 19:32
  */
object Scala_WordCount {
  def main(args: Array[String]): Unit = {

    val arr = Array[String]("haha xixi","xixi haha haha")

    val strings = arr.flatMap(_.split(" "))

    val tuple = strings.map(x=>(x,1)).groupBy(_._1).map(x=>(x._1,x._2.length)).toList

    tuple.foreach(println)

  }
}
