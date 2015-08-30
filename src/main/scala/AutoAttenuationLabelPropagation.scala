/**
 * Created by cucma_000 on 2015/1/8.
 */

package com.wtist.spark.graphx.lib

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

/** Label Propagation algorithm. */
object AutoAttenuationLabelPropagation {
  /**
   * Run static Label Propagation for detecting communities in networks with attenuation.
   *
   * Each node in the network is initially assigned to its own community. At every superstep, nodes
   * send their community affiliation to all neighbors and update their state to the mode community
   * affiliation of incoming messages.
   *
   * ALPA is a standard community detection algorithm for graphs. It is very inexpensive
   * computationally, although (1) convergence is not guaranteed and (2) one can end up with
   * trivial solutions (all nodes are identified into a single community).
   *
   * The edge attribute type should be Double!
   *
   * @param graph the graph for which to compute the community affiliation
   * @param stability the percentage of vertices which won't change its label any more
   * @param m the coefficient of degree
   * @param del the value to minus from the supernode's hopescore in every propagation step
   * implementation, the algorithm will run for exactly this many supersteps.
   *
   * @return a graph with vertex attributes containing the label of community affiliation
   */
  def run[VD: ClassTag](sc:SparkContext, graph: Graph[VD, Double], stability: Double, m: Double, del: Double): Graph[Long, Double] = {
    val g = graph.mapVertices { case (vid, _) => vid.toString }
    val degree = graph.degrees
    val verticeNum = graph.vertices.count().toDouble
    println("vertice number : " + verticeNum.toLong)
    var dG = g.joinVertices[Int](degree)((_, ed, deg) => ed + "-" +deg).mapVertices { case (vid, str) => val arr = str.split("-"); (arr(0).toLong, arr(1).toLong, 1.0, arr(0).toLong)}

    def sendMessage(e: EdgeTriplet[(Long, Long, Double, Long), Double]) = {
      Iterator((e.srcId, Map(e.dstAttr._1 -> e.dstAttr._3*math.pow(e.dstAttr._2, m)*e.attr.toDouble)), (e.dstId, Map(e.srcAttr._1 -> e.srcAttr._3*math.pow(e.srcAttr._2, m)*e.attr.toDouble)))
    }

    def mergeMessage(count1: Map[Long, Double], count2: Map[Long, Double])
    : Map[Long, Double] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0.0)
        val count2Val = count2.getOrElse(i, 0.0)
        i -> (count1Val + count2Val)
      }.toMap
    }

    def vertexProgram(vid: VertexId, attr: (Long, Long, Double, Long), message: Map[Long, Double]) = {
      if (message.isEmpty) (attr._1, attr._2, attr._3, attr._1) else {
        val selected = message.maxBy(_._2)
        val label = selected._1
        if (label == attr._1) (label, attr._2, selected._2, attr._1) else {
          // a node should have the basic ability to vote, we set it to be 0.1, you can choose another threshold as you like
          //if(selected._2 - del > 0) (label, attr._2, selected._2 - del) else (label, attr._2, 0.0)
          (label, attr._2, selected._2 - del, attr._1)
        }
      }
    }

    val initialMessage = Map[Long, Double]()

    var stop = false
    var step = 0
    var max = 0
    var num = 0L
    while (!stop) {
      step += 1
      dG = Pregel(dG, initialMessage, maxIterations = 1)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage)
      val stable = dG.vertices.filter(x => x._2._1 == x._2._4).count()
      if (stable/verticeNum >= stability) stop = true
      println("stable vertice number : " + stable)
      println("step : " + step + "   stability : " + (stable/verticeNum))
      val statistic = dG.vertices.map(v => (v._2._1, 1)).reduceByKey(_ + _).cache()
      max = statistic.map(x => x._2).max()
      num = statistic.map(x => x._1).distinct().count()
      println("max community member count : " + max)
      println("community number : " + num)
      /*dG.vertices.map(x => (x._1, x._2._1, x._2._2, x._2._3)).collect().foreach(println)
      println("*********************************************************************************************")*/
    }
    println("total steps : " + step)
    dG.mapVertices((vid, attr) => attr._1)
  }
}

