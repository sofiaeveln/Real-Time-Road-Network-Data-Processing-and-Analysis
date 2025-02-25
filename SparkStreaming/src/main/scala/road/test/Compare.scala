package road.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 *  处理 路网数据的 溢写代码
 */
object Compare {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(conf)

    val edges = sc.textFile("E:\\体会\\code\\SparkStreaming\\src\\main\\scala\\resources\\edges")
    val nodes = sc.textFile("E:\\体会\\code\\SparkStreaming\\src\\main\\scala\\resources\\nodes")
    val inter = sc.textFile("E:\\体会\\code\\SparkStreaming\\src\\main\\scala\\resources\\inter_roads")

    val e = edges.map(x => x.split("\t")(5)).flatMap(x => x.split(" ")).collect().toSet
    val n = nodes.map(x => x.split(" ")(1) + "," + x.split(" ")(2)).collect().toSet
    val i = inter.map(_.split(" ")(0)).collect()

//    val sa = e.intersect(n).size
    //val ie = e.intersect(i).size
  //size  val in = n.intersect(i).size
    //println(e.size + "---" + n.size + "----" + sa)
    //println(ie + "-----" + in)
    println(i.size + "---" + i.toSet.size) //159275---29606

    e.zipWithIndex

  }
}
