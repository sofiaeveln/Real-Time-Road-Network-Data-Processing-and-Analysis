package generate

import scala.util.Random

import java.util.Properties
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

/**
 *  kafka produccer 向kafka 发送 随机数 ，模拟 路径的起点终点
 */
object producer {
  def main(args: Array[String]): Unit = {
    val kafkaProp = new Properties()

    kafkaProp.put("bootstrap.servers", "westgis104:9092, westgis105:9092")
    kafkaProp.put("acks", "1")
    kafkaProp.put("retries", "3")
    //kafkaProp.put("batch.size", 16384)//16k
    kafkaProp.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProp.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](kafkaProp)
    while (true) {
      for (i <- 1 to 520) { // 这个参数设置每秒发送多少条
        val record = new ProducerRecord[String, String]("sp", Random.nextInt(58040).toString
          + "," + Random.nextInt(58040).toString)
        producer.send(record, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (metadata != null) {
              println("发送成功")
            }
            if (exception != null) {
              println("消息发送失败")
            }
          }
        })

      }
      Thread.sleep(1200) // 这个参数设置每次发送间隔
    }
    producer.close()
  }
}
