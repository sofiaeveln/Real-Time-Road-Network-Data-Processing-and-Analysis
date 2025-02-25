package counsumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Milliseconds, Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

// kafka consumer 测试用
object collecter {

  def main(args: Array[String]): Unit = {

    val Array(brokers, groupId, topics) = Array("westgis104:9092", "cx", "SPARK_identity_20_500_10_1593760932946")

    val sparkConf = new SparkConf().setAppName("sparkSp")
      .setMaster("local[*]")

    val context = new SparkContext(sparkConf)

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val ssc = new StreamingContext(context, Minutes(2))

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
      //.map(r => (r.key().split(":")(0).toLong, r.value().split(":")(1).toLong))
/*
    import spark.implicits._

    messages.foreachRDD{ rdd =>
      rdd.toDF()
    }*/
    messages.print()
    ssc.start()
    //ssc.stop()
  }

}
