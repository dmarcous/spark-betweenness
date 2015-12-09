package kBC

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.Finders
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD
import scala.collection.mutable.HashMap

class KBetweennessTest extends FlatSpec 
{
    val appName="kBC"
    val sparkMode="local"
    val conf = new SparkConf().setAppName(appName).setMaster(sparkMode);
    val sc = new SparkContext(conf);
    
    val vertices: RDD[(VertexId, Int)] =
    sc.parallelize(Array(
                    (1L, 0),
                    (2L, 0),
                    (3L, 0),
                    (4L, 0),
                    (5L, 0),
                    (6L, 0),
                    (7L, 0)
                    ))
    // Create an RDD for edges
    val edges: RDD[Edge[Int]] =
      sc.parallelize(Array(
                    Edge(1L, 2L, 0),
                    Edge(1L, 3L, 0),
                    Edge(2L, 4L, 0),
                    Edge(3L, 4L, 0),
                    Edge(3L, 5L, 0),
                    Edge(4L, 6L, 0),
                    Edge(5L, 7L, 0)
                    ))
    val defaultVertex = 0
    // Build the initial Graph
    val graph = Graph(vertices, edges, defaultVertex)
    
  " getNeighbourMap " should " return neighbour map for graph" in
  {
    val vlist : List[VertexId] =
      List(1L, 2L, 3L, 4L, 5L, 6L, 7L)
    val elist : List[(VertexId, VertexId)] =
      List((1L,2L),(1L,3L),(2L,4L),(3L,4L),(3L,5L),(4L,6L),(5L,7L))
    val neighbourList : HashMap[VertexId, List[VertexId]] = 
      KBetweenness.getNeighbourMap(vlist, elist)
      
    neighbourList(1L) should contain only (2L, 3L)
    neighbourList(2L) should contain only (1L, 4L)
    neighbourList(3L) should contain only (1L, 4L, 5L)
    neighbourList(4L) should contain only (2L, 3L, 6L)
    neighbourList(5L) should contain only (3L, 7L)
    neighbourList(6L) should contain only (4L)
    neighbourList(7L) should contain only (5L)
    
    neighbourList.foreach(println)
  }
   
   " createKGraphlets " should " return 1 graphlets" in
  {
    val k = 1
    
    val kGraphletsGraph =
        KBetweenness.createKGraphlets(graph, k)
        
    val graphlets = kGraphletsGraph.vertices.collect() 
    
    val sortedGraphlets = 
      graphlets.toList.sortWith((x,y) => x._1 < y._1)
      
    sortedGraphlets(0)._1 should equal (1L)
    sortedGraphlets(0)._2._1 should equal (0.0)
    sortedGraphlets(0)._2._2 should contain only (2L, 3L)
    sortedGraphlets(0)._2._3 should contain only ((1L,2L), (1L,3L))
  }
    
}