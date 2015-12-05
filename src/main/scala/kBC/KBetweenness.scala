package kBC

import scala.reflect.ClassTag
import org.apache.spark.Logging

import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD

object KBetweenness 
{
  def run[VD: ClassTag, ED: ClassTag](
            graph: Graph[VD, ED], k: Int): Graph[Double, Double] =
  {
      val kBCGraph: Graph[Double, Double] = null
      
      kBCGraph
  }
  
  def createKGraphlets() = 
  {
    
  }
  
  def computeBetweenessForGraphlet(k: Int, vlist: List[VertexId], elist: List[Edge[Any]]):  List[(VertexId, Double)] = 
  {
    null
  }
  
  def aggregateGraphletsBetweennessScores() =
  {
    
  }

}