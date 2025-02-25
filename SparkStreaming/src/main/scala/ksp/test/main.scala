package ksp.test


import edu.ufl.cise.bsmock.graph.spark.LRUCache

import scala.collection.JavaConverters._
import edu.ufl.cise.bsmock.graph.{Edge, Graph}
import edu.ufl.cise.bsmock.graph.util.{Dijkstra, DijkstraNode, Path, ShortestPathTree}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Milliseconds, Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 *  时隔太久，忘了溢写东西 哈哈哈哈哈哈哈
 *  说一下总体思路， 读取hdfs路网数据 生成图， 把图广播出去
 *  利用 generatee类里的-producer 模拟查询
 *  配有 动态广播 和 缓存机制 在 edu.ufl.cise.bsmock.graph.spark包 下
 *  关于 缓存 和动态广播 ，当初测试的时候可以 使用，但最终 老师 说 测试性能不能开启缓存， 于是 我把 加了缓存和动态广播的代码注释了，
 *  然后手贱把注释删了，  现在 想不起当初的脑回路了， 但 在此包下另一个类 TestLazyEppstein 下 有 使用这两个东西的影子，感兴趣的自己研究一下
 *  我已经看不懂我的代码了~~~~~
 *  最终结果写入hbase
 */
object main {

  def main(args: Array[String]): Unit = {

    val Array(brokers, groupId, topics) = Array("westgis104:9092", "c", "sp")

    val sparkConf = new SparkConf().setAppName("sparkSp")
//      .setMaster("local[*]")
    .set("spark.scheduler.mode","FAIR")
    .set("spark.streaming.concurrentJobs",args(1))  //args(0)
    .set("spark.streaming.kafka.maxRatePerPartition", args(2))

    val context = new SparkContext(sparkConf)
    context.setLogLevel("ERROR")

    val tablename = "sp"
    val columnFamily = "info"
    val familyBytes = columnFamily.getBytes()

    context.hadoopConfiguration.set("hbase.zookeeper.quorum", "westgis104,westgis105")
    context.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    context.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = Job.getInstance(context.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])


    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val ssc = new StreamingContext(context, Milliseconds(1000))
    //ssc.checkpoint(".")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)).map(r =>
      (r.value().split(",")(0), r.value().split(",")(1)))

    //val graphFilename = "E:\\体会\\code\\SparkStreaming\\graph2.txt"
    val graphFilename = args(0)

    //   messages.map{ x => TaskContext.getPartitionId()}.print(50)
    val graph = new Graph()

    val road = context.textFile(graphFilename).map { line =>
      val arrayLine = line.split(",")
      new Edge(arrayLine(0), arrayLine(1), arrayLine(2).toDouble)
    }.collect().toList.asJava

    graph.addEdges(road)
    val broadcastGraph = context.broadcast(graph)

    messages.map( x=> x)
    messages.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val result = rdd.mapPartitions { partition =>
          var cache = List[(ImmutableBytesWritable, Path)]()
          val g = broadcastGraph.value
          while (partition.hasNext) {
            val record = partition.next()
            val source = record._1
            val target = record._2

            val timeStart: Long = System.currentTimeMillis
            val path = Dijkstra.shortestPath(g, source, target)
            val timeFinish: Long = System.currentTimeMillis

            val key = timeFinish - timeStart
            val put = new Put(Bytes.toBytes(key + System.currentTimeMillis()))

            val value = "a" //有个 bug 没改 ，结果有空不能写入，以字符串测试写
            put.addColumn(familyBytes, source.getBytes(), value.getBytes())
            cache.::((new ImmutableBytesWritable, put))
          }
          cache.iterator
        }
        result.saveAsNewAPIHadoopDataset(job.getConfiguration)
        //println("aaaaaaaa")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
