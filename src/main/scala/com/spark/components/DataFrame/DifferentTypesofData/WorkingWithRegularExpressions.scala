package com.spark.components.DataFrame.DifferentTypesofData

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithRegularExpressions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RegularExpressions")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath(args,"/retail-data/by-day/2010-12-01.csv"))
    df.printSchema()
    df.createOrReplaceTempView("dfTable")
    import org.apache.spark.sql.functions.{col, regexp_replace}
    val sampleColors = Seq("black", "white", "red", "green", "blue")
//    val regexString = sampleColors.map(m=>m.toUpperCase).mkString("|")
    val regexString = sampleColors.map(_.toUpperCase).mkString("|")
    println("regexString -> "+regexString)
    df.select(
      regexp_replace(col("Description"),regexString,"COLOR").alias("color_clean"),
      col("Description")
    ).show(2)

    spark.sql(
      """ select
        | regexp_replace(Description,'BLACK|WHITE|RED|GREEN|BLUE','COLOR') as color_clean ,Description from dfTable
        |""".stripMargin).show(3)

    println("****** contains *****")
    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("DESCRIPTION").contains("WHITE")
    df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .where("hasSimpleColor")
      .select("Description")
      .show(2,false)

    spark.sql(
      """ select description from dfTable
        |where Description ='BLACK' OR  Description ='WHITE' """.stripMargin )
      .show(2,false)

    spark.stop()
  }
}
