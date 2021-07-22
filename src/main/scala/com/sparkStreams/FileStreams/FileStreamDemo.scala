package com.sparkStreams.FileStreams

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object FileStreamDemo extends Serializable {


  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("File Streaming Demo")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    val rawDF = spark.readStream
      .format("json")
      .option("path", CommonUtils.getInputFilePath(args,"/streamInput/json/"))
      .load()

    val explodeDF = rawDF.selectExpr(
      "InvoiceNumber", "CreatedTime", "StoreID", "PosID",
      "CustomerType", "PaymentMethod", "DeliveryType",
      "DeliveryAddress.City", "DeliveryAddress.State",
      "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    explodeDF.printSchema()
    import org.apache.spark.sql.functions.expr
    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")
    flattenedDF.printSchema()

    val invoiceWriteQuery = flattenedDF.writeStream
      .format("json")
      .option("path", CommonUtils.getOutputFilePath(args,"/streamOutput/json/"))
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("append")
      .queryName("Flattened Invoice Writer")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Flattened Invoice Writer Started...")

    invoiceWriteQuery.awaitTermination()

  }
}
