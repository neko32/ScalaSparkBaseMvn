package tanuneko

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.io.{IOUtils, SequenceFile, Text, IntWritable}

import scala.annotation.switch
import scala.util.Random

/**
  * Created by neko32 on 2016/01/10.
  */
object RequestLogGen {

  val r = new Random(System.currentTimeMillis)

  def main(args:Array[String]) = {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val id = new IntWritable
    val value = new Text
    val w = SequenceFile.createWriter( conf,
      SequenceFile.Writer.file(new Path(REQ_LOG_FILE)),
      Writer.compression(CompressionType.BLOCK, new DefaultCodec),
      Writer.keyClass(classOf[IntWritable]),
      Writer.valueClass(classOf[Text]))

    for(i <- 0 to 500) {
      id.set(r.nextInt(10000))
      value.set(genInterest)
      w.append(id, value)
    }
    IOUtils.closeStream(w)
  }

  def genInterest = {
    (r.nextInt(3): @switch) match {
      case 0 => "IT"
      case 1 => "Politics"
      case 2 => "Gossip"
    }
}


}
