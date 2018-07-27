package cn.edu360.spark05.Sort

import org.apache.spark.{SparkConf, SparkContext}

//4继承  Ordered  Serializable
// 不在封装成样例类   封装成元祖
//sortby的时候在封装 splitData.sortBy(x=>new Person4(x._2,x._3)).foreach(println)
object SortDemo4 {
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

    //直接在这里封装
   // splitData.sortBy(x=>new Person4(x._1,x._2,x._3)).foreach(println)
    splitData.sortBy(x=>new Person4(x._2,x._3)).foreach(println)

  }
}

class Person4(val name:String,val age:Int,val fv:Int) extends Ordered[Person4] with Serializable {

  def this(age:Int,fv:Int){
    this("xxx",age,fv)
  }

  override def compare(that: Person4) = {
    if(this.fv==that.fv){
      //年龄升序
      this.age-that.age
    }else{
      //fv降序
      that.fv-this.fv
    }

  }

  override def toString = s"Person($name, $age, $fv)"
}

