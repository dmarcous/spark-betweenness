package com.centrality.kBC

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.PartitionStrategy

object kBCDriver 
{
  def main(args: Array[String])
  {
    // Create spark context
    val appName="kBCDriver"
    val conf = new SparkConf().setAppName(appName)//.setMaster(master)
    val sc = new SparkContext(conf)

    // Graph partition params
    val DEFAULT_K = 2
    val DEFAULT_EDGE_PARTITIONS=60
    val DEFAULT_CANONICAL_ORIENTATION=true
    val k = args(0).toInt
    println("k : " + k)
    val canonicalOrientation = DEFAULT_CANONICAL_ORIENTATION
    val numEdgePartitions = args(1).toInt
    
    // Input params
    val DEFAULT_INPUT_DIR="/tmp/input/"
    val DEFAULT_INPUT_FILE_NAME="edge_list.txt"
    val inputDir = args(2)
    val inputFileName = args(4)
    val inputPath = inputDir+inputFileName
    println("inputPath : " + inputPath)
    
    // Output params
    val DEFAULT_OUTPUT_DIR="/tmp/output/"
    val DEFAULT_V_OUTPUT_FILE=List(inputFileName,"kbc",k,"vertices").mkString("_")+".txt"
    val DEFAULT_E_OUTPUT_FILE=List(inputFileName,"kbc",k,"edges").mkString("_")+".txt"
    val outputDir = args(3)
    val outputVerticesFileName = sc.hadoopConfiguration.get("outputVerticesFileName", DEFAULT_V_OUTPUT_FILE)
    val outputEdgesFileName = sc.hadoopConfiguration.get("outputEdgesFileName", DEFAULT_E_OUTPUT_FILE)
    val outputVerticesPath = sc.hadoopConfiguration.get("outputVerticesPath", outputDir+outputVerticesFileName)
    val outputEdgesPath = sc.hadoopConfiguration.get("outputEdgesPath", outputDir+outputEdgesFileName)
    println("outputVerticesPath : " + outputVerticesPath)
    println("outputEdgesPath : " + outputEdgesPath)
    
    // Read graph
    val graph = GraphLoader.edgeListFile(sc, inputPath, canonicalOrientation, numEdgePartitions).partitionBy(PartitionStrategy.EdgePartition2D)
    println(Calendar.getInstance().getTime().toString + " vertices : " + graph.vertices.count())
    println(Calendar.getInstance().getTime().toString + " edges : " + graph.edges.count())
    
    // Run kBC
    println(Calendar.getInstance().getTime().toString + ": start kBC")
    val kBCGraph = 
      KBetweenness.run(graph, k)
    
    // Save graph to file
    println(Calendar.getInstance().getTime().toString + ": saving results ") 
    kBCGraph.vertices.coalesce(1).saveAsTextFile(outputVerticesPath)
    kBCGraph.edges.coalesce(1).saveAsTextFile(outputEdgesPath)
  }
}