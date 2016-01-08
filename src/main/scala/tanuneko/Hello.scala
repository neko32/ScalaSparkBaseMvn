package tanuneko

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by neko32 on 2016/01/07.
  */
object Hello {
  def main(args:Array[String]): Unit = {
    val src = "/user/neko32/spark/study1/test.txt"
    val dest = "/user/neko32/spark/study1/result"
    val hadoopConf = new Configuration
    val fs = FileSystem.get(hadoopConf)
    fs.delete(new Path(dest), true)
    val conf = new SparkConf().setMaster("local").setAppName("MyTest")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(src)
    rdd.flatMap(_.split(","))
    .map((_, 1))
    .reduceByKey(_ + _)
    .saveAsTextFile(dest)
  }
}
