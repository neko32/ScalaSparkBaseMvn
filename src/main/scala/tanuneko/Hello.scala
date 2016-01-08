package tanuneko

import org.apache.spark.SparkContext

/**
  * Created by neko32 on 2016/01/07.
  */
object Hello {
  def main(args:Array[String]): Unit = {
    def sc = new SparkContext
    val rdd = sc.textFile("/user/neko32/spark/study1/test.txt")
    rdd.flatMap(_.split(","))
    .map((_, 1))
    .reduceByKey(_ + _)
    .saveAsTextFile("/user/neko32/spark/study1/result")
  }
}
