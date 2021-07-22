package com.spark.ProblemSolving

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Problem : Identify the Average for sequence numbers 1 - 10
 */
object Problem1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Problem1")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val rdd = spark.sparkContext.parallelize(1 to 10)
    //    rdd.foreach(println)
    val avg = rdd.reduce(_ + _).toDouble / rdd.count()
    println("RDD Avg -> " + avg)

    spark.range(1, 10).toDF("num")
      .selectExpr("round(avg(cast(num as float)),2)")
      .show()

    spark.stop()
  }
}
