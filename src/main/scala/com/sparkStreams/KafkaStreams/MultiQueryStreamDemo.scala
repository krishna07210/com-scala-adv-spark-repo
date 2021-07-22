package com.sparkStreams.KafkaStreams

import com.config.CommonUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object MultiQueryStreamDemo {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  //  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Multi Query Streams Demo")
      .master("local[3]")
      .getOrCreate()


    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      )))),
    ))

    //Note: If we put the read in place of readStream it will not loop into micro batches.
    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest")
      .load()

    // Here Value is Binary so we need to convert it to String
    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), schema).alias("value"))
    valueDF.printSchema()

    val notificationDF = valueDF
      .select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount")
      .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

    //val kafkaTargetDF = notificationDF.selectExpr("InvoiceNumber as key", "to_json(struct(*)) as value")
    // Serialize the data to Json
    val kafkaTargetDF = notificationDF.selectExpr("InvoiceNumber as key",
      """
        |to_json(named_struct('CustomerCardNo', CustomerCardNo,
        |'TotalAmount', TotalAmount,
        |'EarnedLoyaltyPoints', TotalAmount * 0.2
        |)) as value""".stripMargin)

    /*
    val notificationWriterQuery = kafkaTargetDF.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("checkpointLocation", "chk-point-dir")
      .start()
    */

    val notificationWriterQuery = kafkaTargetDF
      .writeStream
      .queryName("Notification Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "notifications")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/notify")
      .start()


    val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
      "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

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
      .option("checkpointLocation", "chk-point-dir/flatten")
      .outputMode("append")
      .queryName("Flattened Invoice Writer")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()


    logger.info("Waiting for Queries")
    notificationWriterQuery.awaitTermination()
  }
}
