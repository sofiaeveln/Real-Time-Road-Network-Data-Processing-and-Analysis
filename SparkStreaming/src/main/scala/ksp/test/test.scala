package ksp.test

import scala.collection.JavaConverters._
import edu.ufl.cise.bsmock.graph.{Edge, Graph}
import edu.ufl.cise.bsmock.graph.util.{Dijkstra, DijkstraNode, ShortestPathTree}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * 测试，， 同样有点看不懂自己代码， 感觉以前我有点东西，现在没了。
 */
object test {


  def main(args: Array[String]): Unit = {

    val Array(brokers, groupId, topics) = Array("westgis104:9092", "c", "sp")

    val sparkConf = new SparkConf().setAppName("sparkSp").setMaster("local[*]")

    val context = new SparkContext(sparkConf)
    context.setLogLevel("ERROR")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val tablename = "sp"
    val configuration = HBaseConfiguration.create()
    configuration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    configuration.set("hbase.zookeeper.quorum", "westgis105,westgis104")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")


    // 初始化job，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val columnFamily = "info"
    val familyBytes = columnFamily.getBytes()

    while (true) {
      val ssc = new StreamingContext(context, Seconds(1))
      //ssc.checkpoint("E:\\体会\\code\\SparkStreaming\\checkpoint")

      val messages = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)).map(r => (r.key(), r.value()))

      val graphFilename = "E:\\体会\\code\\SparkStreaming\\graph2.txt"
      //val graphFilename = args(0)

      val graph = new Graph()

      val road = context.textFile(graphFilename).map { line =>
        val arrayLine = line.split(",")
        new Edge(arrayLine(0), arrayLine(1), arrayLine(2).toDouble)
      }.collect().toList.asJava

      graph.addEdges(road)
      val broadcastGraph = context.broadcast(graph)

      /*    val stream = messages.groupByKey().flatMap { records =>
            val source = records._1
            val targets = records._2

            targets.map { target =>
              val timeStart: Long = System.currentTimeMillis
              val path = Dijkstra.shortestPath(broadcastGraph.value, source, target)
    //          val path = Dijkstra.shortestPath(nodes, shortestPathTree, target)
              val timeFinish: Long = System.currentTimeMillis
              (timeFinish - timeStart, path)
            }
          }*/

      //finally
      messages.foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          val result = rdd.map { records =>
            val source = records._1
            val target = records._2

            val timeStart: Long = System.currentTimeMillis
            val path = Dijkstra.shortestPath(broadcastGraph.value, source, target)
            val timeFinish: Long = System.currentTimeMillis

            val key = timeFinish - timeStart
            val put = new Put(Bytes.toBytes(key + System.currentTimeMillis()))

            val value = "a"
            put.addColumn(familyBytes, source.getBytes(), value.getBytes())
            (new ImmutableBytesWritable, put)
          }
          result.saveAsHadoopDataset(jobConf)
          println("aaaaaaaa")
        }
      }

      //stream.print()
      ssc.start()
      ssc.awaitTerminationOrTimeout(1000 * 60 * 5)
      ssc.stop(false, true)
    }
  }
}
