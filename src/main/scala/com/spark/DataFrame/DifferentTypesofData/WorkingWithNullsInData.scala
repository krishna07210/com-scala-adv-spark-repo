package com.spark.DataFrame.DifferentTypesofData

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithNullsInData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithNullsInData")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath("/retail-data/by-day/2010-12-01.csv"))
    df.printSchema()
    df.createOrReplaceTempView("dfTable")
    println("*******  Coalesce()  *******")
    import org.apache.spark.sql.functions.{coalesce, col}
    df.select(coalesce(col("Description"), col("CustomerId"))).show()
    println("**** ifnull, nullIf, nvl, and nvl2 *****")
    spark.sql(
      """ SELECT
        |ifnull(null, 'return_value'),
        |nullif('value', 'value'),
        |nullif('value','value1'),
        |nvl(null, 'return_value'),
        |nvl2('not_null', 'return_value', 'else_value'),
        |nvl2(null,'return_value','else_value' )
        |FROM dfTable LIMIT 1""".stripMargin).show()

    println("*******  drop  *********")
    df.na.drop()
    df.na.drop("any")
    spark.sql("""select * from dfTable where Description is not null""")
    df.na.drop("all")
    println("****  apply this to certain sets of columns by passing in an array of columns  ***")
    df.na.drop("all", Seq("StockCode", "InvoiceNo"))

    println("***** fill: fill one or more columns with a set of values *****")
    df.na.fill("All Null values become this string")
    df.na.fill(5, Seq("StockCode", "InvoiceNo"))
    val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
    df.na.fill(fillColValues)

    println("*****  replace *****")
    df.na.replace("Description", Map("" -> "UNKNOWN"))
  }
}
