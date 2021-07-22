package com.sparkStreams.KafkaStreams

import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.functions.{col, expr, to_json, from_json, struct, sum}
import org.apache.spark.sql.types._

object KafkaAvroSourceDemo {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  //  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Kafka Avro Source Demo")
      .master("local[3]")
      .getOrCreate()

    //Note: If we put the read in place of readStream it will not loop into micro batches.
    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoice-items")
      .option("startingOffsets", "earliest")
      .load()

    // Here Value is Binary so we need to convert it to String
    val avroSchema = new String(Files.readAllBytes(Paths.get("avro-schemas/invoice-items")))
    val valueDF = kafkaSourceDF.select(from_avro(col("value"), avroSchema).alias("value"))
    valueDF.printSchema()

    val rewardsDF = valueDF.filter("value.CustomerType == 'PRIME'")
      .groupBy("value.CustomerCardNo")
      .agg(sum("value.TotalValue").alias("TotalPurchases"),
        sum(expr("value.TotalValue * 0.2")).alias("AggregatedRewards"))
    rewardsDF.printSchema()

    val kafkaTargetDF = rewardsDF.select(expr("CustomerCardNo as key"),
      to_json(struct("TotalPurchases", "AggregatedRewards")).alias("value"))

    val invoiceWriterQuery = kafkaTargetDF
      .writeStream
      .queryName("Rewards Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "customer-rewards")
      .outputMode("update") //append Exception in thread "main" org.apache.spark.sql.AnalysisException: :Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
      .option("checkpointLocation", "chk-point-dir/Avro")
      .start()

    /** Output :
     * {"TotalPurchases":102557.0,"AggregatedRewards":20511.400000000012}
     * {"TotalPurchases":117690.0,"AggregatedRewards":23538.00000000001}
     */
    logger.info("Kafka Avro Source Demo Query Started")
    invoiceWriterQuery.awaitTermination()
  }
}
