package com.spark.components.DataFrame.DifferentTypesofData

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithBooleans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithBooleans")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath(args,"/retail-data/by-day/2010-12-01.csv"))
    df.printSchema()
    df.createOrReplaceTempView("dfTable")
    println("********  Convert Native types to Spark types:  **********")
    import org.apache.spark.sql.functions.lit
    df.select(lit(5), lit("five")).show(2)
    spark.sql(
      """SELECT 1 , "five" from dfTable limit 2 """
    ).show(2)

    println("********  Working with Booleans  **********")
    import org.apache.spark.sql.functions.col
    df.where(col("InvoiceNo").equalTo(536365))
      .select(col("InvoiceNo"), col("Description"))
      .show(5, false)
    spark.sql(
      """SELECT InvoiceNo,Description from dfTable WHERE InvoiceNo = 536365 """)
      .show(2, false)

    df.where("InvoiceNo <> 536365")
      .select(col("InvoiceNo"), col("Description"))
      .show(1, false)
    spark.sql(
      """SELECT InvoiceNo,Description from dfTable WHERE InvoiceNo != 536365 LIMIT 1""")
      .show(false)

    println("***  Boolean expressions with multiple parts ***")
    val priceFilter = col("UnitPrice") > 600
    val descriptionFilter = col("Description").contains("POSTAGE")
    df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descriptionFilter))
      .show(false)
    spark.sql(
      """ SELECT * FROM dfTable WHERE StockCode in ("DOT")
        |and (UnitPrice > 600  OR  Description = "POSTAGE") """.stripMargin)
    spark.sql(
      """ SELECT * FROM dfTable WHERE StockCode in ("DOT")
        |and (UnitPrice > 600  OR  instr(Description,"POSTAGE") >=1 ) """.stripMargin).show(false)

    println("*** To filter a DataFrame, you can also just\nspecify a Boolean column ***")
    val DOTCodeFilter = col("StockCode") === "DOT"
    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)

    spark.sql(
      """ select unitPrice ,
        |(StockCode = 'DOT' AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
        | from dfTable
        | where (StockCode = 'DOT' AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))
        |  """.stripMargin).show(3)

    import org.apache.spark.sql.functions.{expr, not}
    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)
    println(" ---> 1 ")
    spark.sql(""" select Description, UnitPrice from dfTable where Not UnitPrice <=250 """).show(5)
    spark.stop()
  }
}
