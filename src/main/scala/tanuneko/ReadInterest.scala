package tanuneko

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by neko32 on 2016/01/10.
  */
object ReadInterest {
  def main(args:Array[String]): Unit = {
    val hadoopConf = new Configuration
    val fs = FileSystem.get(hadoopConf)
    val conf = new SparkConf().setMaster("local").setAppName("MyTest")
    val sc = new SparkContext(conf)
    val userProfileRaw = sc.sequenceFile[Int,String](USER_PROFILE_FILE)
      .partitionBy(new HashPartitioner(100)).persist
    val userProfile = userProfileRaw mapValues(_.split(",").toSet)
    //userProfile foreach println
    val reqLog = sc.sequenceFile[Int,String](REQ_LOG_FILE)
    //reqLog foreach println

    val joined = userProfile.join(reqLog)
    joined foreach println
    val offInterest = joined.filter {
      case (id, (interestInProf, interestRequested)) =>
        !interestInProf.contains(interestRequested)
    }
    if(fs.exists(new Path(OFF_INTEREST_FILE))) {
      fs.delete(new Path(OFF_INTEREST_FILE), true)
    }
    offInterest.saveAsTextFile(OFF_INTEREST_FILE)
  }
}
