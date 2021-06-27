name := "com-scala-adv-spark-repo"
organization := "com.spark.learning"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.1"
val mainClassName = "com.spark"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies



