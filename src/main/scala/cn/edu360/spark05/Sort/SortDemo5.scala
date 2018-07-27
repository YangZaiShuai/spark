package cn.edu360.spark05.Sort

import org.apache.spark.{SparkConf, SparkContext}

//5  对象类不再继承ordered
object SortDemo5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sort")
    val sc = new SparkContext(conf)

    val data = sc.makeRDD(List("dd 20 99","laoduan 35 96","laozhao 30 96","huige 28 9999"))

    val splitData = data.map({
      t =>
        val split = t.split(" ")
        val name = split(0)
        val age = split(1).toInt
        val fv = split(2).toInt
        (name, age, fv)
    })

    //使用隐式转换  达到可以比较的目的
    //隐式值有两种方式
    //一
    implicit val ord = new Ordering[Person5]{
      override def compare(x: Person5, y: Person5) = {
        //降序
        y.fv-x.fv
      }
    }
  //  splitData.sortBy(x=>new Person5(x._1,x._2,x._3)).foreach(println)

    //二
    implicit object ord2 extends Ordering[Person5]{
      override def compare(x: Person5, y: Person5) = {
        x.fv-y.fv
      }
    }
    splitData.sortBy(x=>new Person5(x._1,x._2,x._3)).foreach(println)

    //*********如果有俩,优先使用隐式object************

  }
}


//当它没有实现特质ordered时候,是怎么排序的呢?
//使用隐式转换  达到可以比较的目的



class Person5(val name:String,val age:Int,val fv:Int) extends Serializable {

  override def toString = s"Person($name, $age, $fv)"
}

