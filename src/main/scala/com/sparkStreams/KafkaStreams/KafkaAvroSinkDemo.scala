package com.sparkStreams.KafkaStreams

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json, struct}
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.types._

object KafkaAvroSinkDemo {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  //  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Kafka Avro Sink Demo")
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

    val kafkaTargetDF = flattenedDF.select(expr("InvoiceNumber as key"),
      to_avro(struct("*")).alias("value"))

    val invoiceWriterQuery = kafkaTargetDF
      .writeStream
      .queryName("Flattened Invoice Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "invoice-items")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/Avro")
      .start()

    logger.info("Kafka Avro Sink Started")
    invoiceWriterQuery.awaitTermination()
  }
}
