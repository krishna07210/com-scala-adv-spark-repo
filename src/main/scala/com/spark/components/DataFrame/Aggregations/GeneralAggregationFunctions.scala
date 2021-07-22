package com.spark.components.DataFrame.Aggregations

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 * All aggregations are available as functions, in addition to the special cases that can appear on
 * DataFrames or via .stat
 */
object GeneralAggregationFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GeneralAggregationFunctions")
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

    import org.apache.spark.sql.functions.{count, countDistinct, approx_count_distinct}
    println("*** 1 -> count ***")
    df.select(count(col("StockCode"))).show()
    spark.sql(""" select count(StockCode) from dfTable """).show()

    println("*** 2 -> countDistinct ***")
    df.select(countDistinct(col("StockCode"))).show()
    spark.sql(""" select count(distinct StockCode) from dfTable """).show()
    spark.sql(""" select count(distinct *) from dfTable """).show()

    println("*** 3 -> approx_count_distinct ***")

    df.select(approx_count_distinct(col("StockCode"), 0.1)).show()
    spark.sql(""" select approx_count_distinct(StockCode ,0.1) from dfTable """).show()

    println("*** 4 -> first and last ***")
    import org.apache.spark.sql.functions.{first, last}
    df.select(first(col("StockCode")), last(col("StockCode"))).show()
    spark.sql(""" select first(StockCode) , last(StockCode) from dfTable """).show()

    println("*** 5 -> min & max & sum & sumDistinct & avg  ***")
    import org.apache.spark.sql.functions.{min, max, sum, sumDistinct, avg, expr}
    df.select(
      min(col("Quantity")).alias("min_transaction_amt"),
      max(col("Quantity")).alias("max_purchase_amt"),
      count(col("Quantity")).alias("total_transactions"),
      sum(col("Quantity")).alias("total_purchases"),
      sumDistinct(col("Quantity")).alias("unique_total_purchases"),
      avg(col("Quantity")).alias("avg_purchases"),
      expr("mean(Quantity)").alias("mean_purchases")
    )
      .selectExpr(
        "min_transaction_amt",
        "max_purchase_amt",
        "total_transactions",
        "total_purchases",
        "unique_total_purchases",
        "total_purchases / total_transactions",
        "avg_purchases",
        "mean_purchases"
      )
      .show()

    spark.sql(""" select min(Quantity) , max(Quantity) from dfTable """).show()

    spark.stop()
  }
}
