package com.spark.components.RDDs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Caching {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Caching")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)


    val text: String = "Spark The Definitive Guide : Big Data Processing Made Simple"
    val words = spark.sparkContext.parallelize(text.split(" "), 2)
    words.cache()
    println(words.getNumPartitions)
    println(words.getStorageLevel)
    spark.stop()
  }
}
