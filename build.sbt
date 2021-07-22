name := "com-scala-adv-spark-repo"
organization := "com.spark.learning"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.1"
val mainClassName = "com.spark"


val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  // https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0",
  "joda-time" % "joda-time" % "2.10.1",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  // https://mvnrepository.com/artifact/org.apache.kafka/kafka
  "org.apache.kafka" %% "kafka" % "2.6.0",
  // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
  "org.apache.kafka" % "kafka-clients" % "2.5.0",
  // https://mvnrepository.com/artifact/mysql/mysql-connector-java
  "mysql" % "mysql-connector-java" % "8.0.16",
  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
  //  "org.apache.hadoop" % "hadoop-aws" % "3.2.1"


)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)


libraryDependencies ++= sparkDependencies ++ testDependencies


artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}

