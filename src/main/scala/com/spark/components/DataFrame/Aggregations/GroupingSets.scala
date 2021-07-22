package com.spark.components.DataFrame.Aggregations

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GroupingSets {
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
    import org.apache.spark.sql.functions.{col, to_date, sum, max, dense_rank, rank}
    import org.apache.spark.sql.expressions.Window
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val dfWithDate = df.withColumn("date",
      to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")
    dfWithDate.show(false)
    val dfNoNull = dfWithDate.drop()
    println("*** 1 -> GROUPING SETS  only available in sql")
    dfNoNull.createOrReplaceTempView("dfNoNull")
    spark.sql(
      """
        |SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
        |GROUP BY customerId, stockCode
        |ORDER BY CustomerId DESC, stockCode DESC
        |""".stripMargin).show(false)
    spark.sql(
      """
        |SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
        |GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
        |ORDER BY CustomerId DESC, stockCode DESC
        |""".stripMargin).show(false)

    println("*** 2 -> Rollups ***")
    val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    rolledUpDF.show()
    rolledUpDF.where("Country IS NULL").show()
    rolledUpDF.where("Date IS NULL").show()

    println("*** 3 -> Cube ***")
    dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
      .select("Date", "Country", "sum(Quantity)")
      .orderBy("Date").show()

    println("*** 4 -> Grouping Metadata **")
    import org.apache.spark.sql.functions.{grouping_id, expr}
    dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
      .orderBy(expr("grouping_id()").desc)
      .show()

    println("*** 5 -> Pivot ***")
    val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
    pivoted.where("date > '2011-12-05'")
      .select("date" ,"`USA_sum(Quantity)`").show()
    spark.stop()
  }
}
