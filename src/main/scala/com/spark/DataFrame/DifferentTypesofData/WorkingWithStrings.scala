package com.spark.DataFrame.DifferentTypesofData

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithStrings {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithStrings")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath("/retail-data/by-day/2010-12-01.csv"))
    df.printSchema()
    df.createOrReplaceTempView("dfTable")

    println(" ***** String manipulation  *******")
    import org.apache.spark.sql.functions._
    df.select(col("Description"),
      initcap(col("Description")),
      lower(col("Description")),
      upper(col("Description"))).show(2)
    spark
      .sql(
        """select
          |initcap(Description),
          |lower(Description),
          |upper(Description) from dfTable
          |""".stripMargin).show(2)
    df.select(
      ltrim(lit(" HELLO ")).as("ltrim"),
      rtrim(lit(" HELLO ")).as("rtrim"),
      trim(lit(" HELLO ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lp"),
      rpad(lit("HELLO"), 10, " ").as("rp")).show(2)
    spark.sql(
      """select
        |ltrim(' HELLLOOOO '),
        |rtrim(' HELLLOOOO '),
        |trim(' HELLLOOOO '),
        |lpad('HELLOOOO ', 3, ' '),
        |rpad('HELLOOOO ', 10, ' ') from dfTable
        |""".stripMargin).show(2)
  }
}
