package com.spark.components.PairRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ControllingPartitions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ControllingPartitions")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val words = spark.sparkContext
      .parallelize("Spark The Definitive Guide: Big Data Processing Made Simple".split(" "),
        2)

    println("*** 1 ->  coalesce ***")
    println(words.getNumPartitions)
    println(words.coalesce(1).getNumPartitions)

    println("*** 2 ->  repartition ***")
    println(words.repartition(10).getNumPartitions)
    spark.stop()
  }
}
