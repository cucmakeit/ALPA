/**
 * Created by cucma_000 on 2015/1/8.
 */
package com.wtist.spark.graphx.ALPAtest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import com.wtist.spark.graphx.lib.AttenuationLabelPropagation
import com.wtist.spark.graphx.lib.AutoAttenuationLabelPropagation

object ALPA{
  def main (args: Array[String]) {
    //val maxSteps = args(0).toInt
    val stability = args(0).toDouble
    val m = args(1).toDouble
    val del = args(2).toDouble
    val conf = new SparkConf().setAppName("ALPA")
    val sc = new SparkContext(conf)
    val graph: Graph[Long, Double] = GraphGenerators.logNormalGraph(sc, numVertices = 10000, numEParts = 100).mapEdges(e => e.attr.toDouble)
    val g = AutoAttenuationLabelPropagation.run(sc, graph, stability, m, del)
    g.vertices.take(30).foreach(println)
    g.vertices.map(x => x._1 + "," + x._2).saveAsTextFile(args(3))
  }
}



