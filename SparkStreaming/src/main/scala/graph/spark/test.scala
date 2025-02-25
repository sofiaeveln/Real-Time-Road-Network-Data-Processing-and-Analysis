package graph.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}

/**
 *  利用spark graph做最短路径，不知道可不可以用，做参考
 */
object test681 {

  //屏蔽不必要的日志显示在终端上
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
  def main(args: Array[String]) {


    val conf=new SparkConf()
      // .setMaster("local")//启动本地化计算
      .setAppName("zdlj")//设置本程序名称

    // conf.set("spark.testing.memory", "9971859200")//后面的值大于512m即可

    val sc = new SparkContext(conf)
    //读取edges和vertices文件
    //测试
     val bian = sc.textFile("hdfs://10.103.104.148:9000/616work/road/edgtest.txt")
     val dingdian = sc.textFile("hdfs://10.103.104.148:9000/616work/road/vetest.txt")
    //正式
//    val bian = sc.textFile("hdfs://10.103.104.148:9000/616work/road/newnew_edge6666.txt")
//    val dingdian = sc.textFile("hdfs://10.103.104.148:9000/616work/road/newnew_vertices999999999.txt")
    // val bian = sc.textFile("D://newnewlu/edgtest.txt")
    // val dingdian = sc.textFile("D://newnewlu/vetest.txt")


    //定点
    val vertices = dingdian.map{e=>
      val fields = e.split(",")
      (fields(0).toLong,fields(1))
    }


    //边
    val edges = bian.map { e =>
      val fields = e.split(",")
      Edge(fields(0).toLong,fields(1).toLong,fields(2).toDouble)
    }

    val graph = Graph(vertices,edges,",").persist()


    //输入起点与终点

    print("请输入起点ID：")
    val sourceId : VertexId = scala.io.StdIn.readInt()


    //起始时间
    val start = System.currentTimeMillis()

    ////初始化图形，使除根以外的所有顶点都具有无限距离。
    val initialGraph: Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[VertexId](sourceId))
      else (Double.PositiveInfinity, List[VertexId]()))

    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(

      // 顶点程序
      (id, juli, newjuli) => if (juli._1 < newjuli._1) juli else newjuli,

      // 发送消息
      a333 => {
        if (a333.srcAttr._1 < a333.dstAttr._1 - a333.attr) {
          Iterator((a333.dstId, (a333.srcAttr._1 + a333.attr, a333.srcAttr._2 :+ a333.dstId)))
        } else {
          Iterator.empty
        }
      },
      //合并消息
      (a, b) => if (a._1 < b._1) a else b)

    print("请输入终点ID：")
    val end_ID = scala.io.StdIn.readInt()

    print("到达的点,长度,起始点,终点\n")
    print("  ")

    println(sssp.vertices.collect.filter{case(id,v) => id == end_ID}.mkString("\n"))


    //程序结束的时间
    val end = System.currentTimeMillis()


    print("你查询的是点"+sourceId+"到点:"+end_ID+"\n")
    print("*************************************\n")
    print("本次查询时间"+(end-start)+"毫秒\n")
    print("\n\n\n")

    sc.stop()
  }


}
