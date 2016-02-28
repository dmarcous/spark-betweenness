# spark-beetweenness

k Betweenness Centrality (kBC) algorithm for Spark using GraphX

## Details

Computing k Betweenness Centrality (kBC) on arbitraty graphs using GraphX.

### Steps  

  1. Using Pregel API to create small k-graphlets
  2. Local beetweenness contribution calculation for ecah vertex to other vertices using Brandes algorithm for calculating BC.
  3. Aggregating results (reduce) from all vertices to conclude each vertex betweenness score.
  4. Assemble a graph similar to the original graph that contains the kBC score for each node.

### Notes 

  The method above ilustrated that spark-beetweenness works best for graphs with small diameter.
  We actually hold all k-graphlets in memory for Brandes calculation as they are the core of parallelizing this algorithm.
  Therefore, we manage to compute kBC on millions of nodes and vertices with large diameter graphs (such as road networks), but faily miserabely to do so on small diameter graphs (such as social networks).

## Requirements

Spark 1.5+.
Scala 2.10.

## Usage

### Scala API

```scala
// import needed for the .avro method to be added
import com.centralitly.kBC.KBetweenness
		
val graph = Graph(users, relationships, defaultUser)

// Run kBC to get result graph
val kBCGraph = 
      KBetweenness.run(graph, 3)
```

### Running kBC from command line

You can run kBC directly form command line using spark-submit.

Parameters for kBC :
- **k** k for kBC computation
- **numEdgePartitions** number of partitions when loading graph, recommended ( **num-executors:** * 2 ) - 6
- **inputDir** HDFS input dir location
- **outputDir** HDFS output dir location
- **inputFileName** input file name containing edge_list for graph (as stated in GraphX GraphLoader.edgeListFile)


    /usr/lib/spark/bin/spark-submit --class com.centrality.kBC.kBCDriver --executor-cores 1 --executor-memory 10000M --master yarn-cluster --num-executors 28 --conf spark.driver.memory=10000m --conf spark.driver.extraJavaOptions="-Xms4000m -Xmx10000m" --conf spark.executor.extraJavaOptions="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf spark.kryo.registrationRequired=false --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.yarn.maxAppAttempts=1 --conf spark.task.maxFailures=10 /tmp/kbc_2.10-1.0.jar 4 50 /tmp/input/ /tmp/output/ loc-brightkite_edges.txt

When using kBC this way, it is highly recommended to tune these parameters for your own benefit :
- **executor-cores:** recommended to 1 
- **executor-memory:** 
- **num-executors:** as far as memory of the cluster grants, given each executor needs X **executor-memory:**
- **spark.driver.extraJavaOptions** tune memory requirements
- **spark.executor.extraJavaOptions** tune GC if needed
- **spark.task.maxFailures** recommended larger than 1, tasks fail for out of memory sometimes for large graphs if tuning isn't right
    

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html),
which is automatically downloaded by the included shell script.  To build a JAR file simply run
`sbt package` from the project root.



