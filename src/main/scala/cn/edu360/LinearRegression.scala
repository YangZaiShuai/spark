package cn.edu360

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.{LinearRegressionModel, LinearRegressionWithSGD}
object LinearRegression {
  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创建SparkContext
    val conf = new SparkConf().setMaster("local[4]").setAppName("LinearRegression")
    val sc = new SparkContext(conf)

    val path = "data/sample_linear_regression_data.txt"

    //通过提供的工具类加载样本文件
    val data = MLUtils.loadLibSVMFile(sc,path).cache()
    //或者通过RDD装换加载
    /*val data = sc.textFile(path).map { line =>
      val parts = line.split(' ')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(_.split(":")(1).toDouble)))
    }.cache()*/

    //迭代次数
    val numIterations = 100
    //梯度下降步长
    val stepSize = 0.00000001
    //训练模型
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize)

    //模型评估
    val valuesAndPreds = data.map { point =>
      //根据模型预测Label值
      val prediction = model.predict(point.features)
      println(s"【真实值】：${point.label}      ;【预测值】：${prediction}")
      (point.label, prediction)
    }

    //求均方误差
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("训练模型的均方误差为 = " + MSE)

    //保存模型
    model.save(sc, "data/aaa")
    //重新加载模型
    val sameModel = LinearRegressionModel.load(sc, "data/bbb")

    sc.stop()
  }
}
