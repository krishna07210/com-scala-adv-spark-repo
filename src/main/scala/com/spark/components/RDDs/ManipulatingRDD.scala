package com.spark.components.RDDs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Many transformations mirror the functionality that you find in the Structured
 * APIs. Just as you do with DataFrames and Datasets, you specify transformations on one RDD to
 * create another.
 */
object ManipulatingRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ManipulatingRDD")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    myCollection.foreach(println)
    val words = spark.sparkContext.parallelize(myCollection, 2)

    println("** 1 -> distinct ")
    println("Distinct - " + words.distinct().count())

    println("** 2 -> filter ")
    val filterRDD = words.filter(word => startsWithS(word)).collect()
    filterRDD.foreach {
      println
    }

    println("** 3 -> map ")
    val words2 = words.map(word => (word, word(0), word.startsWith("S")))
    words2.foreach(x => {
      println(x._1 + " - " + x._2 + " - " + x._3)
    })


    println("** 3 -> flatMap ")
    words.flatMap(word => word.toUpperCase.toSeq).take(5).foreach(println)
    words.flatMap(word => word.toUpperCase.toSeq)
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)

    println("** 3 -> sort ")
    words.flatMap(word => word.toUpperCase.toSeq)
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2, ascending = false)
      .collect()
      .foreach(println)

    println("** 4 -> Random Splits ")
    val fiftyFiftySplit = words.randomSplit(Array[Double](0.5,0.5))
    fiftyFiftySplit.foreach(println)

    spark.stop()
  }

  def startsWithS(individual: String): Boolean = {
    individual.startsWith("S")
  }
}
