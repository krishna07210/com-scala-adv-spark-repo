package com.spark.components.PairRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PairRDDOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PairRDDOperations")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val words = spark.sparkContext
      .parallelize("Spark The Definitive Guide: Big Data Processing Made Simple".split(" "),
        2)
    words.foreach(println)
    val pairRDD = words.map(word => (word.toLowerCase, 1))
    pairRDD.foreach { p => println(p._1 + "-" + p._2) }

    println("** 1 -> keyBy ***")
    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
    keyword.foreach(println)

    println("** 2 -> mapValues ***")
    keyword.mapValues(word => word.toUpperCase).collect().foreach(println)

    println("** 3 -> flatMapValues ***")
    keyword.flatMapValues(word => word.toUpperCase).sortByKey().foreach(println)

    println("** 4 -> Extracting Keys and Values **")
    keyword.keys.collect().foreach(println)
    keyword.values.collect().foreach(println)

    println("** 4 -> lookup **")
    keyword.lookup("b").foreach(println)

    println("** 5 -> sampleByKey **")
    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
      .collect()
    import scala.util.Random
    val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKey(true, sampleMap, 6L)
      .collect()
    spark.stop()
  }
}

