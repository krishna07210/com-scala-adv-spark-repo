package com.spark.components.DataFrame.Aggregations

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WindowFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WindowFunctions")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath(args,"/retail-data/all/*.csv"))
      .coalesce(5)
    df.printSchema()
    df.cache()
    df.createOrReplaceTempView("dfTable")
    df.show(3, false)

    import org.apache.spark.sql.functions.{col, to_date, to_timestamp, max, dense_rank, rank}
    import org.apache.spark.sql.expressions.Window
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val dfWithDate = df.withColumn("date",
      to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")
    dfWithDate.show(false)
    val windowSpec = Window
      .partitionBy("CustomerId", "date")
      .orderBy(col("date").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")
      ).show(false)

    spark.sql(
      """SELECT CustomerId, date, Quantity,
        |rank(Quantity) OVER (PARTITION BY CustomerId, date
        |ORDER BY Quantity DESC NULLS LAST
        |ROWS BETWEEN
        |UNBOUNDED PRECEDING AND
        |CURRENT ROW) as rank,
        |dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
        |ORDER BY Quantity DESC NULLS LAST
        |ROWS BETWEEN
        |UNBOUNDED PRECEDING AND
        |CURRENT ROW) as dRank,
        |max(Quantity) OVER (PARTITION BY CustomerId, date
        |ORDER BY Quantity DESC NULLS LAST
        |ROWS BETWEEN
        |UNBOUNDED PRECEDING AND
        |CURRENT ROW) as maxPurchase
        |FROM dfWithDate WHERE CustomerId IS NOT NULL ORDER BY CustomerId""".stripMargin).show()
    spark.stop()
  }
}
