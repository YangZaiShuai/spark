package cn.edu360.spark05.Sort

import org.apache.spark.{SparkConf, SparkContext}

//3  样例类,继承Ordered,
//读取数据封装成对象,然后再sortby  不用继承Serializable

object SortDemo3 {
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

        //new 不 new都可以
        //new PersonCase(name, age, fv)
        PersonCase(name, age, fv)
    })
    splitData.sortBy(x=>x).foreach(println)

  }
}

case class PersonCase(val name:String,val age:Int,val fv:Int) extends Ordered[PersonCase]  {


  override def compare(that: PersonCase) = {
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

