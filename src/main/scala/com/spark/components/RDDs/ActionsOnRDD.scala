package com.spark.components.RDDs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ActionsOnRDD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ActionsOnRDD")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("** 1 -> reduce ")
    println(spark.sparkContext.parallelize(1 to 20).reduce(_ + _))

    val text: String = "Spark The Definitive Guide : Big Data Processing Made Simple"
    val words = spark.sparkContext.parallelize(text.split(" "), 2)
    println(words.reduce(wordLengthReducer))

    println("** 2 -> countApprox")
    val confidence = 0.95
    val timeoutMilliseconds = 400
    words.countApprox(timeoutMilliseconds, confidence)

    println("** 3 -> countApproxDistinct")
    words.countApproxDistinct(0.05)
    words.countApproxDistinct(4,10)

    println("** 3 -> countByValue")
    words.countByValue()

    println("** 4 -> countByValueApprox")
    words.countByValueApprox(1000,0.95)

    println("** 5 -> first")
    words.first()

    println("** 6 -> max and min ")
    spark.sparkContext.parallelize(1 to 10 ).max()
    spark.sparkContext.parallelize(1 to 10 ).min()

    println("** 7 -> take")
    words.take(5)
    words.takeOrdered(5)
    words.top(5)
    val withReplacement = true
    val numberToTake = 6
    val randomSeed = 100L
    words.takeSample(withReplacement, numberToTake, randomSeed)
    spark.stop()
  }


  def wordLengthReducer(leftWord: String, rightWord: String): String = {
    if (leftWord.length > rightWord.length)
      return leftWord
    else
      return rightWord
  }

}
