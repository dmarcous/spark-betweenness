package com.centralitly.kBC

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
    sortedGraphlets(0)._2._2 should contain only (1L, 2L, 3L)
    sortedGraphlets(0)._2._3 should contain only ((1L,2L), (1L,3L))
    
    sortedGraphlets(1)._1 should equal (2L)
    sortedGraphlets(1)._2._1 should equal (0.0)
    sortedGraphlets(1)._2._2 should contain only (1L, 2L, 4L)
    sortedGraphlets(1)._2._3 should contain only ((1L,2L), (2L,4L))
    
    sortedGraphlets(2)._1 should equal (3L)
    sortedGraphlets(2)._2._1 should equal (0.0)
    sortedGraphlets(2)._2._2 should contain only (1L, 3L, 4L, 5L)
    sortedGraphlets(2)._2._3 should contain only ((1L,3L), (3L,4L), (3L,5L))
    
    sortedGraphlets(3)._1 should equal (4L)
    sortedGraphlets(3)._2._1 should equal (0.0)
    sortedGraphlets(3)._2._2 should contain only (2L, 3L, 4L, 6L)
    sortedGraphlets(3)._2._3 should contain only ((2L,4L), (3L,4L), (4L,6L))
    
    sortedGraphlets(4)._1 should equal (5L)
    sortedGraphlets(4)._2._1 should equal (0.0)
    sortedGraphlets(4)._2._2 should contain only (3L, 5L, 7L)
    sortedGraphlets(4)._2._3 should contain only ((3L,5L), (5L,7L))
    
    sortedGraphlets(5)._1 should equal (6L)
    sortedGraphlets(5)._2._1 should equal (0.0)
    sortedGraphlets(5)._2._2 should contain only (4L, 6L)
    sortedGraphlets(5)._2._3 should contain only ((4L,6L))
    
    sortedGraphlets(6)._1 should equal (7L)
    sortedGraphlets(6)._2._1 should equal (0.0)
    sortedGraphlets(6)._2._2 should contain only (5L, 7L)
    sortedGraphlets(6)._2._3 should contain only ((5L,7L))
  }
   
   it should " return 3 graphlets" in
  {
    val k = 3
    
    val kGraphletsGraph =
        KBetweenness.createKGraphlets(graph, k)
        
    val graphlets = kGraphletsGraph.vertices.collect() 
    
    val sortedGraphlets = 
      graphlets.toList.sortWith((x,y) => x._1 < y._1)
      
    sortedGraphlets(0)._1 should equal (1L)
    sortedGraphlets(0)._2._1 should equal (0.0)
    sortedGraphlets(0)._2._2 should contain only (1L, 2L, 3L, 4L, 5L, 6L, 7L)
    sortedGraphlets(0)._2._3 should contain only ((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L), (5L,7L))
    
    sortedGraphlets(1)._1 should equal (2L)
    sortedGraphlets(1)._2._1 should equal (0.0)
    sortedGraphlets(1)._2._2 should contain only (1L, 2L, 3L, 4L, 5L, 6L)
    sortedGraphlets(1)._2._3 should contain only ((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L))
    
    sortedGraphlets(2)._1 should equal (3L)
    sortedGraphlets(2)._2._1 should equal (0.0)
    sortedGraphlets(2)._2._2 should contain only (1L, 2L, 3L, 4L, 5L, 6L, 7L)
    sortedGraphlets(2)._2._3 should contain only ((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L), (5L,7L))
    
    sortedGraphlets(3)._1 should equal (4L)
    sortedGraphlets(3)._2._1 should equal (0.0)
    sortedGraphlets(3)._2._2 should contain only (1L, 2L, 3L, 4L, 5L, 6L, 7L)
    sortedGraphlets(3)._2._3 should contain only ((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L), (5L,7L))
    
    sortedGraphlets(4)._1 should equal (5L)
    sortedGraphlets(4)._2._1 should equal (0.0)
    sortedGraphlets(4)._2._2 should contain only (1L, 2L, 3L, 4L, 5L, 6L, 7L)
    sortedGraphlets(4)._2._3 should contain only ((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L), (5L,7L))
    
    sortedGraphlets(5)._1 should equal (6L)
    sortedGraphlets(5)._2._1 should equal (0.0)
    sortedGraphlets(5)._2._2 should contain only (1L, 2L, 3L, 4L, 5L, 6L)
    sortedGraphlets(5)._2._3 should contain only ((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L))
    
    sortedGraphlets(6)._1 should equal (7L)
    sortedGraphlets(6)._2._1 should equal (0.0)
    sortedGraphlets(6)._2._2 should contain only (1L, 3L, 4L, 5L, 7L)
    sortedGraphlets(6)._2._3 should contain only ((1L,3L), (3L,4L), (3L,5L), (5L,7L))
  }
   
  "computeVertexBetweenessCentrality" should " compute betwenness scores for single vertices" in
  {
    val k = 3
    
    val vertexId1 = 1L
    val vertex1VList = List(1L, 2L, 3L, 4L, 5L, 6L, 7L)
    val vertex1EList = List((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L), (5L,7L))
    val vertex1KBcGraph =
      KBetweenness.computeVertexBetweenessCentrality(vertexId1, vertex1VList, vertex1EList)
      
    val sorted1KBcContibution = 
      vertex1KBcGraph.toList.sortWith((x,y) => x._1 < y._1)
    
    sorted1KBcContibution(0)._1 should equal (2L)
    sorted1KBcContibution(0)._2 should equal (1.0)
    sorted1KBcContibution(1)._1 should equal (3L)
    sorted1KBcContibution(1)._2 should equal (3.0)
    sorted1KBcContibution(2)._1 should equal (4L)
    sorted1KBcContibution(2)._2 should equal (1.0)
    sorted1KBcContibution(3)._1 should equal (5L)
    sorted1KBcContibution(3)._2 should equal (1.0)
    sorted1KBcContibution(4)._1 should equal (6L)
    sorted1KBcContibution(4)._2 should equal (0.0)
    sorted1KBcContibution(5)._1 should equal (7L)
    sorted1KBcContibution(5)._2 should equal (0.0)
    
    val vertexId2 = 2L
    val vertex2VList = List(1L, 2L, 3L, 4L, 5L, 6L)
    val vertex2EList = List((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L))
    val vertex2KBcGraph =
      KBetweenness.computeVertexBetweenessCentrality(vertexId2, vertex2VList, vertex2EList)
      
    val sorted2KBcContibution = 
      vertex2KBcGraph.toList.sortWith((x,y) => x._1 < y._1)
    
    sorted2KBcContibution(0)._1 should equal (1L)
    sorted2KBcContibution(0)._2 should equal (1.0)
    sorted2KBcContibution(1)._1 should equal (3L)
    sorted2KBcContibution(1)._2 should equal (1.0)
    sorted2KBcContibution(2)._1 should equal (4L)
    sorted2KBcContibution(2)._2 should equal (2.0)
    sorted2KBcContibution(3)._1 should equal (5L)
    sorted2KBcContibution(3)._2 should equal (0.0)
    sorted2KBcContibution(4)._1 should equal (6L)
    sorted2KBcContibution(4)._2 should equal (0.0)
    
    val vertexId3 = 3L
    val vertex3VList = List(1L, 2L, 3L, 4L, 5L, 6L, 7L)
    val vertex3EList = List((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L), (5L,7L))
    val vertex3KBcGraph =
      KBetweenness.computeVertexBetweenessCentrality(vertexId3, vertex3VList, vertex3EList)
      
    val sorted3KBcContibution = 
      vertex3KBcGraph.toList.sortWith((x,y) => x._1 < y._1)
      
    sorted3KBcContibution(0)._1 should equal (1L)
    sorted3KBcContibution(0)._2 should equal (0.5)
    sorted3KBcContibution(1)._1 should equal (2L)
    sorted3KBcContibution(1)._2 should equal (0.0)
    sorted3KBcContibution(2)._1 should equal (4L)
    sorted3KBcContibution(2)._2 should equal (1.5)
    sorted3KBcContibution(3)._1 should equal (5L)
    sorted3KBcContibution(3)._2 should equal (1.0)
    sorted3KBcContibution(4)._1 should equal (6L)
    sorted3KBcContibution(4)._2 should equal (0.0)
    sorted3KBcContibution(5)._1 should equal (7L)
    sorted3KBcContibution(5)._2 should equal (0.0)

    val vertexId4 = 4L
    val vertex4VList = List(1L, 2L, 3L, 4L, 5L, 6L, 7L)
    val vertex4EList = List((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L), (5L,7L))
    val vertex4KBcGraph =
      KBetweenness.computeVertexBetweenessCentrality(vertexId4, vertex4VList, vertex4EList)
      
    val sorted4KBcContibution = 
      vertex4KBcGraph.toList.sortWith((x,y) => x._1 < y._1)
    
    sorted4KBcContibution(0)._1 should equal (1L)
    sorted4KBcContibution(0)._2 should equal (0.0)
    sorted4KBcContibution(1)._1 should equal (2L)
    sorted4KBcContibution(1)._2 should equal (0.5)
    sorted4KBcContibution(2)._1 should equal (3L)
    sorted4KBcContibution(2)._2 should equal (2.5)
    sorted4KBcContibution(3)._1 should equal (5L)
    sorted4KBcContibution(3)._2 should equal (1.0)
    sorted4KBcContibution(4)._1 should equal (6L)
    sorted4KBcContibution(4)._2 should equal (0.0)
    sorted4KBcContibution(5)._1 should equal (7L)
    sorted4KBcContibution(5)._2 should equal (0.0)

    val vertexId5 = 5L
    val vertex5VList = List(1L, 2L, 3L, 4L, 5L, 6L, 7L)
    val vertex5EList = List((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L), (5L,7L))
    val vertex5KBcGraph =
      KBetweenness.computeVertexBetweenessCentrality(vertexId5, vertex5VList, vertex5EList)
      
    val sorted5KBcContibution = 
      vertex5KBcGraph.toList.sortWith((x,y) => x._1 < y._1)
    
    sorted5KBcContibution(0)._1 should equal (1L)
    sorted5KBcContibution(0)._2 should equal (0.5)
    sorted5KBcContibution(1)._1 should equal (2L)
    sorted5KBcContibution(1)._2 should equal (0.0)
    sorted5KBcContibution(2)._1 should equal (3L)
    sorted5KBcContibution(2)._2 should equal (4.0)
    sorted5KBcContibution(3)._1 should equal (4L)
    sorted5KBcContibution(3)._2 should equal (1.5)
    sorted5KBcContibution(4)._1 should equal (6L)
    sorted5KBcContibution(4)._2 should equal (0.0)
    sorted5KBcContibution(5)._1 should equal (7L)
    sorted5KBcContibution(5)._2 should equal (0.0)

    val vertexId6 = 6L
    val vertex6VList = List(1L, 2L, 3L, 4L, 5L, 6L)
    val vertex6EList = List((1L,2L), (1L,3L), (2L,4L), (3L,4L), (3L,5L), (4L,6L))
    val vertex6KBcGraph =
      KBetweenness.computeVertexBetweenessCentrality(vertexId6, vertex6VList, vertex6EList)

    val sorted6KBcContibution = 
      vertex6KBcGraph.toList.sortWith((x,y) => x._1 < y._1)
        
    sorted6KBcContibution(0)._1 should equal (1L)
    sorted6KBcContibution(0)._2 should equal (0.0)
    sorted6KBcContibution(1)._1 should equal (2L)
    sorted6KBcContibution(1)._2 should equal (0.5)
    sorted6KBcContibution(2)._1 should equal (3L)
    sorted6KBcContibution(2)._2 should equal (1.5)
    sorted6KBcContibution(3)._1 should equal (4L)
    sorted6KBcContibution(3)._2 should equal (4.0)
    sorted6KBcContibution(4)._1 should equal (5L)
    sorted6KBcContibution(4)._2 should equal (0.0)
        
    val vertexId7 = 7L
    val vertex7VList = List(1L, 3L, 4L, 5L, 7L)
    val vertex7EList = List((1L,3L), (3L,4L), (3L,5L), (5L,7L))
    val vertex7KBcGraph =
      KBetweenness.computeVertexBetweenessCentrality(vertexId7, vertex7VList, vertex7EList)

    val sorted7KBcContibution = 
      vertex7KBcGraph.toList.sortWith((x,y) => x._1 < y._1)      

    sorted7KBcContibution(0)._1 should equal (1L)
    sorted7KBcContibution(0)._2 should equal (0.0)
    sorted7KBcContibution(1)._1 should equal (3L)
    sorted7KBcContibution(1)._2 should equal (2.0)
    sorted7KBcContibution(2)._1 should equal (4L)
    sorted7KBcContibution(2)._2 should equal (0.0)
    sorted7KBcContibution(3)._1 should equal (5L)
    sorted7KBcContibution(3)._2 should equal (3.0)

  }
  
   "run" should " compute betwenness centrality graph" in
  {
     val k = 3
     val kBCGraph = 
       KBetweenness.run(graph, k)
       
     val verticesBetweenness =
       kBCGraph
       .vertices
       .collect()
      
     val sortedVBC = 
       verticesBetweenness.sortWith((x,y) => x._1 < y._1)
    
     sortedVBC(0)._1 should equal (1L)
     sortedVBC(0)._2 should equal (2.0)
     sortedVBC(1)._1 should equal (2L)
     sortedVBC(1)._2 should equal (2.0)
     sortedVBC(2)._1 should equal (3L)
     sortedVBC(2)._2 should equal (14.0)
     sortedVBC(3)._1 should equal (4L)
     sortedVBC(3)._2 should equal (10.0)
     sortedVBC(4)._1 should equal (5L)
     sortedVBC(4)._2 should equal (6.0)
     sortedVBC(5)._1 should equal (6L)
     sortedVBC(5)._2 should equal (0.0)
     sortedVBC(6)._1 should equal (7L)
     sortedVBC(6)._2 should equal (0.0)
  }
   
}