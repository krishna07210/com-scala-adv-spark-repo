package com.spark.components.DataFrame.DataSources

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReadingFromDatabasesInParallel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ReadingFromDatabasesInParallel")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val driver = "org.sqlite.JDBC"
    val path = "/data/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:/${path}"
    val tableName = "flight_info"
    val dbDataFrame = spark.read.format("jdbc")
      .option("url",url)
      .option("dbtable",tableName)
      .option("driver",driver)
      .option("numPartitions",10)
      .load()
    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show()

    val props = new java.util.Properties
    props.setProperty("driver","org.sqlite.JDBC")
    val predicates = Array(
      "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
      "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
    spark.read.jdbc(url, tableName, predicates, props).show()
    spark.read.jdbc(url, tableName, predicates, props).count()
    spark.read.jdbc(url,tableName,predicates,props).rdd.getNumPartitions //2

    println("*** Partitioning based on a sliding window ***")
    val colName = "count"
    val lowerBound = 0L
    val upperBound = 348113L // this is the max count in our database
    val numPartitions = 10

    spark.read.jdbc(url,tableName,colName,lowerBound,upperBound,numPartitions,props)
      .count() // 255
    spark.stop()
  }
}
