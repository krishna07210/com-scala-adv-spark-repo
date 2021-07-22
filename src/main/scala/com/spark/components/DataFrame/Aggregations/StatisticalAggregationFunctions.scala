package com.spark.components.DataFrame.Aggregations

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StatisticalAggregationFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StatisticalAggregationFunctions")
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

    import org.apache.spark.sql.functions.{var_pop, var_samp, stddev_pop, stddev_samp}
    println("*** 1 -> Variance and Standard Deviation ***")
    df.select(var_pop("Quantity"),
      var_samp("Quantity"),
      stddev_pop("Quantity"),
      stddev_samp("Quantity")).show(false)

    spark.sql(
      """select var_pop(Quantity) ,
        | var_samp(Quantity),
        | stddev_pop(Quantity),
        | stddev_samp(Quantity) from dfTable
        |""".stripMargin).show(false)

    println("*** 2 -> skewness and kurtosis ***")
    import org.apache.spark.sql.functions.{skewness, kurtosis}
    df.select(skewness("Quantity"),
      kurtosis("Quantity")).show()
    spark.sql(
      """ SELECT skewness(Quantity),
        |kurtosis(Quantity) FROM dfTable """.stripMargin).show(false)

    println("*** 3 -> Covariance and Correlation ***")
    import org.apache.spark.sql.functions.{corr, covar_pop, covar_samp}
    df.select(corr("InvoiceNo", "Quantity"),
      covar_samp("InvoiceNo", "Quantity"),
      covar_pop("InvoiceNo", "Quantity")).show()

    spark.sql(
      """SELECT corr(InvoiceNo, Quantity), covar_samp(InvoiceNo, Quantity),
        |covar_pop(InvoiceNo, Quantity)
        |FROM dfTable""".stripMargin)

    spark.stop()
  }
}
