package cn.edu360.spark05.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ClickDemo {
  def main(args: Array[String]): Unit = {
    //id 关键字 展示量 点击量

    val conf = new SparkConf()
      .setAppName(getClass.getName)
        .setMaster("local")
    val sc = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("data/impclick.txt")

    val map1= file.flatMap({
      t =>
        val split1 = t.split(",")
        val id = split1(0)
        val keys = split1(1)
        val imp = split1(2).toInt
        val click = split1(3).toInt

        val allKeys = keys.split("[|]")
        val map = allKeys.map({
          keyword =>
            (id, keyword, imp, imp)
        })
        map
    })

    //将id,keyword 当做key
   val grouped: RDD[((String, String), Iterable[(String, String, Int, Int)])] = map1.groupBy(t=>(t._1,t._2))

    val value: RDD[((String, String), (Int, Int))] = grouped.mapValues({
      it =>
        val imp = it.map({
          case (_, _, imp, _) => imp
        })
        val totalimp = imp.sum

        val click = it.map({
          case (_, _, _, click) => click
        })
        val totalClick = click.sum

        (totalimp, totalClick)
    })
    //value.foreach(println)
    value.map(t=>(t._1._1,t._1._2,t._2._1,t._2._2))
        .coalesce(1)
      .sortBy(t=>(t._1,-t._3,-t._4,t._2))
        .foreach(println)
    sc.stop()

  }
}
