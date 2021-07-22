package com.spark.components.DataFrame.Aggregations

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GroupingAggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GroupingAggregation")
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

    println("*** 1 -> Grouping with Expressions  ***")
    import org.apache.spark.sql.functions.{count, expr}
    df.groupBy("InvoiceNo", "CustomerId").count().show(3)
    spark.sql(
      """select InvoiceNo,CustomerId, count(*) cnt
        |from dfTable group by  InvoiceNo,CustomerId""".stripMargin).show(3)

    df.groupBy("InvoiceNo")
      .agg(count("Quantity").alias("quan"),
        expr("count(Quantity)")
      ).show()

    println("*** 2 -> Grouping with Maps  ***")
    df.groupBy("InvoiceNo")
      .agg("Quantity" -> "avg",
        "Quantity" -> "stddev_pop").show()
    spark.sql(
      """SELECT avg(Quantity), stddev_pop(Quantity), InvoiceNo
        |FROM dfTable
        |GROUP BY InvoiceNo""".stripMargin)

    spark.stop()
  }
}
