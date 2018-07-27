package day03.最受欢迎老师

/**
  * @auther create by jhy
  * @date 2018/4/21 16:29
  */
object TestSplit {
  def main(args: Array[String]): Unit = {

    val line = "http://bigdata.edu360.cn/laozhao"
    val strings: Array[String] = line.split("/")
    println(strings.length)
    println(strings(0))
    println(strings(1))
    println(strings(2))
    println(strings(3))


  }
}
