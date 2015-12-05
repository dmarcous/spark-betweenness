package kBC

import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Pregel
import org.apache.spark.graphx.EdgeDirection

object KBetweenness 
{
  def run[VD: ClassTag, ED: ClassTag](
            graph: Graph[VD, ED], k: Int): Graph[Double, Double] =
  {
      val kGraphletsGraph =
        createKGraphlets(graph, k)
        
      val vertexKBcGraph =
        kGraphletsGraph
        .mapVertices((id, attr) => (id, computeVertexBetweenessCentrality(attr._2)))
    
      val kBCGraph: Graph[Double, Double] = 
        null
        //aggregateGraphletsBetweennessScores(vertexKBcGraph)
      
      kBCGraph
  }
  
  def createKGraphlets[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int): Graph[(Double, List[(VertexId, VertexId)]), Double] = 
  {
    val graphContainingGraphlets : Graph[(Double, List[(VertexId, VertexId)]), Double] =
      graph
      // Init edges to hold - Edge Betweenness to 0.0
      .mapTriplets[Double]({x: EdgeTriplet[VD, ED] => (0.0) })
      // Init vertices to hold - Vertex betweenness (0.0), and K distance Edge list (empty)
      .mapVertices( (id, attr) => (0.0, List[(VertexId, VertexId)]()))
      .cache()
      
      def vertexProgram(id: VertexId, attr: (Double,List[(VertexId, VertexId)]), msgSum: List[(VertexId, VertexId)]): (Double, List[(VertexId, VertexId)]) =
        (attr._1, (attr._2.union(msgSum)))
      def sendMessage(edge: EdgeTriplet[(Double, List[(VertexId, VertexId)]), Double]) : Iterator[(VertexId, List[(VertexId, VertexId)])]=
        Iterator((edge.dstId, edge.srcAttr._2))
      def messageCombiner(a: List[(VertexId, VertexId)], b: List[(VertexId, VertexId)]): List[(VertexId, VertexId)] = 
        a.union(b)
      // The initial message received by all vertices in PageRank
      val initialMessage = List[(VertexId, VertexId)]()

      // Execute pregel for k iterations, get all vertices/edges in distance k for every node
      Pregel(graphContainingGraphlets, initialMessage, k, activeDirection = EdgeDirection.Both)(
        vertexProgram, sendMessage, messageCombiner)        
  }
  
  def computeVertexBetweenessCentrality(elist: List[(VertexId, VertexId)]):  List[(VertexId, Double)] = 
  {
    null
  }
  
  def aggregateGraphletsBetweennessScores() =
  {
    
  }

}