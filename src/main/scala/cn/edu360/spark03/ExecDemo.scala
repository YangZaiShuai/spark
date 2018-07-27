package cn.edu360.spark03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object ExecDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ExecDemo")
    val sc = new SparkContext(conf)

    //读数据
    val lines1 = sc.textFile("a.txt")
    val lines2 = sc.textFile("b.txt")
    //切分数据
    val value1 = lines1.map({
      t =>
        val lines = t.split(" ", 2)
        (lines(0), lines(1))
    })
    val value2 =lines2.map({
      t=>
        val lines = t.split(" ",2)
        (lines(0),lines(1))
    })

    //数据组合
    val value: RDD[(String, String)] = value1.union(value2)

    //对values出来,排序,组装
    //按照key分组,分组之后,要合并组装数据
    val groupBykey: RDD[(String, Iterable[String])] = value.groupByKey()


    //对迭代器里的数据组合排序
    val res = groupBykey.mapValues({
      t =>
        val v1 = t.head
        val v2 = t.tail

        val v2Res = if (v2.isEmpty) {
          "null,null,null"
        } else {
/*          val by = v2.toList.sortBy({
            t =>
              t.split(" ")(0)
          })
          by.mkString(" ")*/

          v2.toList.sortWith({
            case(x,y)=>
              x.split(" ")(0)>y.split(" ")(0)
          }).mkString(" ")

        }

        v1 + "," + v2Res
    })

    res.foreach(println)



  }
}
