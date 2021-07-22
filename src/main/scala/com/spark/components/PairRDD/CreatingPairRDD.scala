package com.spark.components.PairRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CreatingPairRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CreatingPairRDD")
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
    spark.stop()
  }
}
