package com.centralitly.kBC

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

object MainRunner 
{
  def main(args: Array[String])
  {
    // Create spark context
    val appName="kBC"
    val sparkMode="local"
    val conf = new SparkConf().setAppName(appName).setMaster(sparkMode);
    val sc = new SparkContext(conf);
    
    // Create sample graph
    //
    // Create an RDD for vertices
    val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                         (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                           Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    
    val kBCGraph = 
      KBetweenness.run(graph, 3)
  }
}