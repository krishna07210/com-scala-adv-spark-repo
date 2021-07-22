package com.sparkStreams.KafkaStreams

import com.sparkStreams.KafkaStreams.KafkaSourceForStreamingQueries.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object KafkaSourceForBatchQueries {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaSourceForStreamingQueries Demo")
      .master("local[3]")
      .getOrCreate()

    // Subscribe to 1 topic defaults to the earliest and latest offsets
    val df1 = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093")
      .option("subscribe", "invoices1")
      .load()
    import spark.implicits._

    df1.printSchema()
    df1.show(false)

    df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .show(false)

    //Subscribe to multiple topics, specifying explicit Kafka offsets
    println("*** Subscribe to multiple topics, specifying explicit Kafka offsets ***")
    val df2 = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoices1")
      .option("startingOffsets", """{"invoices1":{"0":4}}""") // Read from Offset 4 for Partition 0
      //      .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
      //      .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
      .load()
    df2.show(false)

    df2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .show(false)

    // Subscribe to a pattern, at the earliest and latest offsets

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribePattern", "invoice.*")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    spark.stop()
  }
}
