package com.spark.DataFrame.DifferentTypesofData

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithComplexTypes {
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

    df.selectExpr("(Description, InvoiceNo) as Complex", "*").show(2, false)
    println("******* struct ***********")
    import org.apache.spark.sql.functions.{col, struct}
    df.selectExpr("struct(Description,InvoiceNo) as Complex", "*").show(3, false)

    val complexDf = df.select(struct("Description", "InvoiceNo").alias("complex"))
    complexDf.createOrReplaceTempView("complexDfTable")
    complexDf.select("complex.Description").show(4)
    complexDf.select(col("complex").getField("InvoiceNo")).show(3)
    complexDf.select("complex.*").show(2, false)

    spark.sql("""select complex.* from complexDfTable """).show(3)

    println("******  Arrays  ********** ")
    import org.apache.spark.sql.functions.{split, size, array_contains, explode}
    df.select(split(col("Description"), " ")).show(2, false)
    spark.sql(""" SELECT split(Description, ' ') FROM dfTable """).show(3, false)
    df.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[0]").show(4)
    df.select(size(split(col("Description"), " "))).show(2)
    println("**** array_contains *****")
    df.select(col("Description"), array_contains(split(col("Description"), " "), "WHITE")).show(2)

    df.filter("Description = 'WHITE HANGING HEART T-LIGHT HOLDER'")
      .withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "InvoiceNo", "exploded").show(false)

    spark.sql(
      """ select Description ,InvoiceNo , exploded
        |FROM (SELECT * , split(Description," ") as splitted FROM dfTable)
        |LATERAL VIEW explode(splitted) as exploded
        |""".stripMargin).show(2, false)

    println("***** Maps *************")
    import org.apache.spark.sql.functions.{map}
    df.select(map(col("Description"), col("InvoiceNo"))
      .alias("complex_map")).show(3, false)

    spark.sql("""select map(Description,InvoiceNo) complex_map from dfTable """)
      .show(3, false)

    df.where("Description = 'WHITE HANGING HEART T-LIGHT HOLDER'")
      .select(map(col("Description"), col("InvoiceNo"))
        .alias("complex_map"))
      .selectExpr("explode(complex_map)").show(3, false)

  }
}
