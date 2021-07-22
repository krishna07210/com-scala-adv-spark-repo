package com.sparkStreams.KafkaStreams

import com.sparkStreams.KafkaStreams.KafkaAvroSourceDemo.getClass
import com.sparkStreams.StreamingWordCount.logger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object KafkaSourceForStreamingQueries {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaSourceForStreamingQueries Demo")
      .master("local[3]")
      .getOrCreate()

    // Subscribe to 1 topic
    val df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093")
      .option("subscribe", "invoices1")
      .load()
    import spark.implicits._

    df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir/subscribe1")
      .outputMode("update")
      .start()
      .awaitTermination()

    logger.info("Started Query ->1")

    // Subscribe to multiple topics
    val df2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093")
      .option("subscribe", "invoices1,invoices2")
      .load()

    df2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir/subscribe2")
      .outputMode("update")
      .start()
      .awaitTermination()

    logger.info("Started Query ->2")


    // Subscribe to a pattern
    val df3 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093")
      .option("subscribePattern", "invoices.*")
      .load()
    import spark.implicits._
    df3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir/subscribe3")
      .outputMode("update")
      .start()
      .awaitTermination()

    logger.info("Started Query ->3")

  }
}
