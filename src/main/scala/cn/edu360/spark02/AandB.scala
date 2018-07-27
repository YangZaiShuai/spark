package cn.edu360.spark02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AandB {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("FavTeacher")
    val sc = new SparkContext(conf)

    val lines1 = sc.textFile("a.txt")
    val lines2 = sc.textFile("b.txt")

    val value1 = lines1.map({
      line =>
        val split1 = line.split(" ")
        val user = split1(0)
        val arr1 =Tuple2(split1(1), split1(2))
        (user, arr1)
    })


    val value2= lines2.map({
      line =>
        val split1 = line.split(" ")
        val user = split1(0)
        val arr2 = Tuple3(split1(1), split1(2), split1(3))
        (user, arr2)
    })

    val result = value1.leftOuterJoin(value2)

    //val Final = result.groupBy(_._1)


    result.collect().foreach(println)

  }
}
