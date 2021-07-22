package com.sparkStreams.KafkaStreams

import com.sparkStreams.KafkaStreams.KafkaSourceForBatchQueries.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WritingBatchQueriesToKafka {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaSourceForStreamingQueries Demo")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
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
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "invoices-output")
      .save()

    spark.stop()
  }

}
