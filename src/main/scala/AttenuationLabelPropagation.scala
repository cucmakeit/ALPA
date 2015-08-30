/**
 * Created by cucma_000 on 2015/1/7.
 */
package com.wtist.spark.graphx.lib

import scala.reflect.ClassTag
import org.apache.spark.graphx._

/** Label Propagation algorithm. */
object AttenuationLabelPropagation {
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
   * @param maxSteps the number of supersteps of ALPA to be performed. Because this is a static
   * @param m the coefficient of degree
   * @param del the value to minus from the supernode's hopescore in every propagation step
   * implementation, the algorithm will run for exactly this many supersteps.
   *
   * @return a graph with vertex attributes containing the label of community affiliation
   */
  def run[VD: ClassTag](graph: Graph[VD, Double], maxSteps: Int, m: Double, del: Double): Graph[Long, Double] = {
    val g = graph.mapVertices { case (vid, _) => vid.toString }
    val degree = graph.degrees
    val dG = g.joinVertices[Int](degree)((_, ed, deg) => ed + "-" +deg).mapVertices { case (vid, str) => val arr = str.split("-"); (arr(0).toLong, arr(1).toLong, 1.0)}

    def sendMessage(e: EdgeTriplet[(Long, Long, Double), Double]) = {
      Iterator((e.srcId, Map(e.dstAttr._1 -> e.dstAttr._3*(math.pow(e.dstAttr._2, m))*e.attr.toDouble)), (e.dstId, Map(e.srcAttr._1 -> e.srcAttr._3*(math.pow(e.srcAttr._2, m))*e.attr.toDouble)))
    }

    def mergeMessage(count1: Map[Long, Double], count2: Map[Long, Double])
    : Map[Long, Double] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0.0)
        val count2Val = count2.getOrElse(i, 0.0)
        i -> (count1Val + count2Val)
      }.toMap
    }

    def vertexProgram(vid: VertexId, attr: (Long, Long, Double), message: Map[Long, Double]) = {
      if (message.isEmpty) attr else {
        val selected = message.maxBy(_._2)
        val label = selected._1
        if (label == attr._1) (label, attr._2, selected._2) else {
          // a node should have the basic ability to vote, we set it to be 0.1, you can choose another threshold as you like
          if(selected._2 - del > 0.1) (label, attr._2, selected._2 - del) else (label, attr._2, 0.1)
        }
      }
    }

    val initialMessage = Map[Long, Double]()
    Pregel(dG, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
      .mapVertices((vid, attr) => attr._1)
  }
}