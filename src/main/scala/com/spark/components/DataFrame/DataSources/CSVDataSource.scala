package com.spark.components.DataFrame.DataSources

import com.config.CommonUtils
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Commons
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CSVDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CSVDataSource")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("*** Reading CSV Files ***")
    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
    val myManualSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))
    val csvFile = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load(CommonUtils.getInputFilePath(args,"/flight-data/csv/2010-summary.csv"))
    csvFile.show(10)

    val arrayCSV = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load(CommonUtils.getInputFilePath(args,"/flight-data/csv/2010-summary.csv"))
      .take(5)
    println(arrayCSV)
    println("*** Writing CSV File ***")
    csvFile.write.format("csv")
      .mode("overwrite")
      .option("sep", "\t")
      .save(CommonUtils.getOutputFilePath(args,"/csv/my-tsv-file.tsv"))
    spark.stop()
  }
}
