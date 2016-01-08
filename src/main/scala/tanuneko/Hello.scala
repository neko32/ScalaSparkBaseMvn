package tanuneko

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by neko32 on 2016/01/07.
  */
object Hello {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MyTest")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/user/neko32/spark/study1/test.txt")
    rdd.flatMap(_.split(","))
    .map((_, 1))
    .reduceByKey(_ + _)
    .saveAsTextFile("/user/neko32/spark/study1/result")
  }
}
