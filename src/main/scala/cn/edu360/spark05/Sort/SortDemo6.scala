package cn.edu360.spark05.Sort

import org.apache.spark.{SparkConf, SparkContext}

//5  对象类不再继承ordered,也不要隐式转换
object SortDemo6 {
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

    //5------------5------------------
    /** [(String,Int,Int)]   表示的是输入类型的参数
      *  t=>(-t._3,t._2)  表示排序的具体规则   颜值的降序  年龄的升序
      *  Ordering[(Int,Int)] 表示的就是具体规则的参数类型
      */
      implicit val ord = Ordering[(Int,Int)].on[(String,Int,Int)](t=>(-t._3,t._2))
      splitData.sortBy(x=>x).foreach(println)

    //----------------6-----------------------
    val result = splitData.sortBy(t=>(-t._3,t._1.length))
    result.foreach(println)

  }
}





/*
class Person6(val name:String,val age:Int,val fv:Int) extends Serializable {

  override def toString = s"Person($name, $age, $fv)"
}

*/
