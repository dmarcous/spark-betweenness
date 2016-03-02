name := "spark-betweenness"

organization := "com.centrality"

version := "1.0"

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.10.5", "2.11.7")

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"

spName := "dmarcous/spark-betweenness"

sparkVersion := "1.5.2"

sparkComponents += "graphx" 

spAppendScalaVersion := true

EclipseKeys.eclipseOutput := Some("target/eclipse")

/********************
 * Release settings *
 ********************/

credentials += Credentials(Path.userHome / ".sbtcredentials")

spHomepage := "https://github.com/dmarcous/spark-betweenness"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

pomExtra :=
  <url>https://github.com/dmarcous/spark-betweenness</url>
  <scm>
    <url>git@github.com:dmarcous/spark-betweenness.git</url>
    <connection>scm:git:git@github.com:dmarcous/spark-betweenness.git</connection>
  </scm>
  <developers>
    <developer>
      <id>dmarcous</id>
      <name>Daniel Marcous</name>
      <url>https://github.com/dmarcous</url>
    </developer>
    <developer>
      <id>yotamsandbank</id>
      <name>Yotam Sandbank</name>
      <url>https://github.com/yotamsandbank</url>
    </developer>
  </developers>
