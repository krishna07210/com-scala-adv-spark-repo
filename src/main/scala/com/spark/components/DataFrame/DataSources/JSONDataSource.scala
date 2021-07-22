package com.spark.components.DataFrame.DataSources

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object JSONDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JSONDataSource")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val myManualSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))
    val jsonFile = spark.read
      .format("json")
      .option("mode", "FAILFAST")
      //      .option("inferSchema","true")
      .schema(myManualSchema)
      .load(CommonUtils.getInputFilePath(args,"flight-data/json/2010-summary.json"))

    jsonFile.write
      .format("json")
      .mode("overwrite")
      .save(CommonUtils.getOutputFilePath(args,"/json/my-json-file.json"))
    spark.stop()
  }
}
