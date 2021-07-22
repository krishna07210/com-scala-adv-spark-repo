package com.spark.components.SharedVariables

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BroadcastVariables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BroadcastVariables")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)

    val supplementalData = Map("Spark" -> 1000,"Definitive" -> 200,"Big" ->300,"Simple"->100)
    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)
    suppBroadcast.value
    println("Broadcast Value -> "+suppBroadcast.value)
    words.map(word => (word,suppBroadcast.value.getOrElse(word,0)))
      .sortBy(wordPair => wordPair._2)
      .collect()
      .foreach(println)

    spark.stop()
  }
}
