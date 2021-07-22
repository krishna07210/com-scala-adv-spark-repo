package com.spark.components.RDDs

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CreatingRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CreatingRDDs")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    spark.range(500).rdd
    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
    import spark.implicits._
    spark.range(10).rdd.toDF()

    println("** 1 -> Create an RDD from a Local Collection **")
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    myCollection.foreach(println)
    val words = spark.sparkContext.parallelize(myCollection, 2)
    println("** Name this RDD to show up in the Spark UI **")
    words.setName("myWords")
    println("RDD Name : " + words.name)

    println("** 2 -> Create From Data Sources **")
    val flights = spark.sparkContext.textFile(CommonUtils.getInputFilePath(args,"/flight-data/csv/"))
    flights.foreach(println)

    val flightsWholefile = spark.sparkContext.wholeTextFiles(CommonUtils.getInputFilePath(args,"/flight-data/csv/"))
    flightsWholefile.foreach(x=> println(x._1 + " - " +x._2))
    spark.stop()
  }
}
