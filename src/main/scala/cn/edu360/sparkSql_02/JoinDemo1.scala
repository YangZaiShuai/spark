package cn.edu360.sparkSql_02

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinDemo1 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    import session.implicits._

    //关联  两张表
    //person 个人信息  name age fv address
    //归属地   address addname

    //需要创建俩dataset
    val person: Dataset[String] = session.createDataset(List(/*"bq 40 79 hn", */"jnl 35 90 heb", "hb 40 85 qd", "cangls 35 99 jp"))
    val pDs = person.map({
      t =>
        val splits = t.split(" ")
        val name = splits(0)
        val age = splits(1)
        val fv = splits(2)
        val pro = splits(3)
        (name, age, fv, pro)
    })
    val personDF = pDs.toDF("name", "age", "fv", "address")

    val nation: Dataset[String] = session.createDataset(List("hn,河南省", "heb,哈尔滨", "qd,山东省"/*, "jp,日本省"*/))
    val nationDs: Dataset[(String, String)] = nation.map({
      t =>
        val splits = t.split(",")
        val address = splits(0)
        val aname = splits(1)
        (address, aname)
    })
    val nationDF: DataFrame = nationDs.toDF("address1", "addname")

    //关联查询

    //sql类型查询
    //创建临时表
    personDF.createTempView("per")
    nationDF.createTempView("nat")
    //    session.sql("select * from per,nat where per.address=nat.address")
    //      .show()
//    session.sql("select * from per join nat on per.address=nat.address order by fv desc")
     // .show()

import org.apache.spark.sql.functions._
    //dsl 查询
    // 第一种  比较麻烦
    // 如果两个表中的列名一致，需要指定df的名称
    val joinDF1 = personDF.join(nationDF)
    //joinDF1.where($"address" === $"address1").show()
    //joinDF1.where(personDF("address")===nationDF("address"))
     // .show()

    //    joinDF1.filter(personDF("address")===nationDF("address"))
    //      .show()

    //第二种  直接指定关联的列
    // 列相同的时候
//    personDF.join(nationDF,"address").orderBy($"age").filter($"age"<40)
//      .show()

    //列名不相同
//    personDF.join(nationDF,$"address"===$"address1").orderBy($"age").filter($"age">30)
//      .show()

    //join 默认使用的是inner join
    /*Type of join to perform. Default `inner`. Must be one of:
   *`inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   * `right`, `right_outer`, `left_semi`, `left_anti`.*/

    //left join
//    personDF.join(nationDF,$"address"===$"address1","left")
//      .show()
    //right join
//    personDF.join(nationDF,$"address"===$"address1","right")
//      .show()
 //right join
    personDF.join(nationDF,$"address"===$"address1","cross")
      .show()



    session.close()


  }
}
