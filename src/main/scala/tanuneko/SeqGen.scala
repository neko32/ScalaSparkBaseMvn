package tanuneko

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.io.{IOUtils, IntWritable, SequenceFile, Text}

import scala.annotation.switch
import scala.util.Random

/**
  * Created by neko32 on 2016/01/10.
  */

case class UserTopic(topic:Text)

object SeqGen {

  val r = new Random(System.currentTimeMillis)

  def main(args:Array[String]): Unit = {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val id = new IntWritable
    val value = new Text
    val w = SequenceFile.createWriter( conf,
                                        SequenceFile.Writer.file(new Path(USER_PROFILE_FILE)),
                                        Writer.compression(CompressionType.BLOCK, new DefaultCodec),
                                        Writer.keyClass(classOf[IntWritable]),
                                        Writer.valueClass(classOf[UserTopic]))

    for(i <- 0 to 10000) {
      id.set(i)
      value.set(genInterest)
      w.append(id, UserTopic(value))
    }
    IOUtils.closeStream(w)
  }

  def genInterest = {
    (r.nextInt(7): @switch) match {
      case 0 => "IT"
      case 1 => "Politics"
      case 2 => "Gossip"
      case 3 => "IT,Politics"
      case 4 => "IT,Gossip"
      case 5 => "Politics,Gossip"
      case 6 => "IT,Politics,Gossip"
    }
  }
}

