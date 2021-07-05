package com.spark.DataFrame.DifferentTypesofData

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithNumbers {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithNumbers")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath("/retail-data/by-day/2010-12-01.csv"))
    df.printSchema()
    df.createOrReplaceTempView("dfTable")
    println("--> 1 DF ")
    import org.apache.spark.sql.functions.{col, expr, pow}
    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
    println("--> 2 DF ")
    df.selectExpr(
      "CustomerId",
      "(POWER((Quantity * UnitPrice),2.0) +5 ) as realQuantity").show(2)
    println("--> 1 SQL ")
    spark.sql(
      """ select CustomerId ,(POWER((Quantity * UnitPrice), 2.0) + 5)  as realQuantity
        |FROM dfTable  """.stripMargin).show(2)
    println("***** round function  *****")

    import org.apache.spark.sql.functions.{bround, lit, round}
    df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
    spark.sql("select round(2.5), bround(2.5)").show(2)

    println("***  Compute the correlation of two columns  ****")
    import org.apache.spark.sql.functions.corr
    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr("Quantity", "UnitPrice")).show()
    spark.sql(""" select corr(Quantity,UnitPrice) from dfTable """).show()

    println("***** describe ******")
    df.describe().show()

    println("****** approxQuantile Method ******")
    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    df.stat.approxQuantile(colName, quantileProbs, relError)
    println("***** cross-tabulation or frequent item pairs  ****")
    df.stat.crosstab("StockCode", "Quantity").show()
    df.stat.freqItems(Seq("StockCode", "Quantity")).show()

    println("***** monotonically_increasing_id ******")
    df.select(org.apache.spark.sql.functions.monotonically_increasing_id()).show()

  }

}
