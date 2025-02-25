package ksp.test

import java.util

import scala.collection.JavaConverters._
import edu.ufl.cise.bsmock.graph.ksp.LazyEppstein
import edu.ufl.cise.bsmock.graph.spark.{BroadcastStringPeriodicUpdater, LRUCache}
import edu.ufl.cise.bsmock.graph.util.{Dijkstra, DijkstraNode, Path, ShortestPathTree}
import edu.ufl.cise.bsmock.graph.{Edge, Graph}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

/**
 *  再见
 */
object TestLazyEppstein extends Serializable {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val context = new SparkContext(sparkConf)
    context.setLogLevel("ERROR")

    while (true) {
      val ssc = new StreamingContext(context, Seconds(1))
     // ssc.checkpoint("E:\\体会\\code\\SparkStreaming\\checkpoint")
      val initialRDD = ssc.sparkContext.parallelize(List(("13", "21"), ("8888", "9999"), ("1", "9999")))
      var queueRDDs = new mutable.Queue[RDD[(String, String)]]()
      queueRDDs +== initialRDD

      val graphFilename = "E:\\体会\\code\\SparkStreaming\\graph2.txt"
      val graph = new Graph()

      val road = context.textFile(graphFilename).map { line =>
        val arrayLine = line.split(",")
        new Edge(arrayLine(0), arrayLine(1), arrayLine(2).toDouble)
      }.collect().toList.asJava

      graph.addEdges(road)
      val broadcastGraph = context.broadcast(graph)

      val mappingFunc = (source: String, target: Option[Iterable[String]], cache: State[(Boolean, ShortestPathTree,LRUCache[String, Path])]) => {
        val timeStart: Long = System.currentTimeMillis
        val graph = broadcastGraph.value
        var path: Path = null
        val i = target.get.iterator

        val state = cache.getOption().getOrElse(false, new ShortestPathTree(source), new LRUCache[String, Path](100))
        val flag = state._1
        val shortestPathTree = state._2
        val lru = state._3

        //更新最小生成树
        if (flag == false) {
          val nodes = graph.getNodes; //邻接表
          import scala.collection.JavaConversions._
          for (nodeLabel <- nodes.keySet) { //不断向树里插入邻接表的key，起点
            val newNode = new DijkstraNode(nodeLabel)
            newNode.setDist(Double.MaxValue)
            newNode.setDepth(Integer.MAX_VALUE)
            shortestPathTree.add(newNode)
          }
          cache.update(true, shortestPathTree, lru)
        }

        var p = List[(Long, Path)]()
        while (i.hasNext) {
          //更新LRU
          val to = i.next()
          if (lru.containsKey(to)) {
            path = lru.get(to)
          } else {
            path = Dijkstra.shortestPath(graph,shortestPathTree, to) //sp
            lru.put(to, path)
            cache.update(true, shortestPathTree, lru)
          }
          val timeFinish: Long = System.currentTimeMillis
          println(timeFinish- timeFinish, path)
          // System.out.println("Operation took " + (timeFinish - timeStart) / 1000.0 + " seconds.")
          p.::((timeFinish - timeStart, path))
        }
        p.iterator
      }


      val stream = ssc.queueStream(queueRDDs).groupByKey().mapWithState(StateSpec.function(mappingFunc)).map( x => x.size)

      stream.print()
      for (i <- 0 to 1000) {
        queueRDDs +== ssc.sparkContext.parallelize(List((Random.nextInt(9).toString, Random.nextInt(9).toString),
          (Random.nextInt(20).toString, Random.nextInt(9).toString),
          (Random.nextInt(20).toString, Random.nextInt(9).toString),
          (Random.nextInt(580).toString, Random.nextInt(9).toString),
          ("8888", "9999"), ("8888", "9999")))
        //Thread.sleep(200) //每2秒发一次数据
      }
      ssc.start()
      ssc.awaitTerminationOrTimeout(1000*60*5)
      ssc.stop(false, true)
    }
  }
}
