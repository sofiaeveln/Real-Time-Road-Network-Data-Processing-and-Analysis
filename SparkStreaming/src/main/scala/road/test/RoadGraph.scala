package road.test

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

case class Node(start:Int, end:Int, weight:String)

object RoadGraph {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(conf)

    val edges = sc.textFile("E:\\体会\\code\\SparkStreaming\\src\\main\\scala\\resources\\edges")
    val nodes = sc.textFile("E:\\体会\\code\\SparkStreaming\\src\\main\\scala\\resources\\nodes")
    val inter = sc.textFile("E:\\体会\\code\\SparkStreaming\\src\\main\\scala\\resources\\inter_roads")

    val e = edges.map(x => x.split("\t")(5)).flatMap(x => x.split(" ")).collect().toSet
    val node = nodes.map(x => x.split(" ")(1) + "," + x.split(" ")(2)).collect().toSet
    val interNodes = inter.map(_.split(" ")(0)).collect().toSet.union(node).toList.sorted
    val interNodesWithIndex = interNodes.zipWithIndex.toMap //zipwithIndex不会打乱顺序

    println(interNodesWithIndex.size)
    //interNodesWithIndex.foreach(println)
    //val edgeNodes = e.zipWithIndex
    //val interNodes = i.zipWithIndex

    val array = ListBuffer[Node]()

    val result = edges.map(x => x.split("\t")(5)).collect().foreach { edge =>
      val arrayNodes = edge.split(" ")
      var tmpStart = ""
      var tmpEnd = ""
      var count = 0
      var distance:Double = 0.0
      var prePoint = ""

      arrayNodes.foreach { n =>
        if (interNodesWithIndex.contains(n)) {
          if (tmpStart == "") {
            tmpStart = n
            //prePoint = n
          }
          else {
            tmpEnd = n
            val finalDistance = (distance + getDistatce(prePoint, tmpEnd)).formatted("%.2f")
            val tmp =  Node(interNodesWithIndex.get(tmpStart).get, interNodesWithIndex.get(tmpEnd).get, finalDistance)
            //if(!array.contains(tmp))
              array += tmp
              val tmp1 = Node(interNodesWithIndex.get(tmpEnd).get, interNodesWithIndex.get(tmpStart).get, finalDistance)
            array += tmp1

            distance = 0.0
            //prePoint = n

            count = count+1
            tmpStart = tmpEnd
          }
        } else {
          distance = distance + getDistatce(prePoint,n)
          //prePoint = n
        }
        prePoint = n
      }
      //println(count)
    }

    val save = array.distinct.sortBy(_.start)

    val writer = new PrintWriter(new File("edge1.txt"))
    var n = 0
    for (i <- save) {
      writer.println("e" + n.formatted("%06d") + ",v" + i.start.formatted("%05d") +",v" + i.end.formatted("%05d") + "," + i.weight)
      n = n+1
    }

    n = 0
    val writer1 = new PrintWriter(new File("node1.txt"))
    for(no <- interNodesWithIndex){
      writer1.println("v" + no._2.formatted("%05d") + "," + no._1)
      n = n+1
    }
    writer.close()
    writer1.close()
    println(array.distinct.size)


  }

  def getDistatce(start:String, end:String)= {
    val Array(lon1,lat1) = start.split(",").map(_.toDouble)
    val Array(lon2, lat2) = end.split(",").map(_.toDouble)
    if (lat1 != 0 && lon1 != 0 && lat2 != 0 && lon2 != 0) {
      val R = 6378.137
      val radLat1 = lat1 * Math.PI / 180
      val radLat2 = lat2 * Math.PI / 180
      val a = radLat1 - radLat2
      val b = lon1 * Math.PI / 180 - lon2 * Math.PI / 180
      val s = 2 * Math.sin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
      BigDecimal.decimal(s * R).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else {
      BigDecimal.decimal(0).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
  }
}
