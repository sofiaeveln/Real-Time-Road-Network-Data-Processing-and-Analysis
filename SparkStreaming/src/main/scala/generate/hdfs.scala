package generate

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取 hdfs 文件， 写入kafka
 * 参数看 协同文档
 */
object hdfs extends Serializable {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("kafkHdfs")
    //  .setMaster("local[*]")

    val context = new SparkContext(sparkConf)
    context.setLogLevel("ERROR")

    val fileName = args(0)

    val kafkaProp = new Properties()

    kafkaProp.put("bootstrap.servers", args(1)) //kafka broker地址
    kafkaProp.put("acks", "1")
    kafkaProp.put("retries", "3")
    //kafkaProp.put("batch.size", 16384)//16k
    kafkaProp.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProp.put("value.serializer", classOf[StringSerializer].getName)

    //粤B005VX,114.062897,22.638599,2017-03-25 07:03:04,13
    context.textFile(fileName).repartition(args(5).toInt).foreachPartition { rows =>
      val producer = new KafkaProducer[String, String](kafkaProp)
      var count = 0
      while (rows.hasNext) {
        if(count == args(3).toInt){
          Thread.sleep(args(4).toLong)
          count = 0
        }
        count = count + 1
        val row = rows.next()
        val record = new ProducerRecord[String, String](args(2), getKey(row), row.toString) //这里修改key 和 value
        producer.send(record, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (metadata != null) {
              println("发送成功")
            }
            if (exception != null) {
              println("消息发送失败")
            }
          }
        }
        )
      }
    }
  }

  //得到key
  def getKey(elem:String):String = {
    elem.split(",")(0)
  }
}
