package com.spark.components.DataFrame.DataSources

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ParquetDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetDataSource")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val parquetFile = spark.read
      .format("parquet")
      .load(CommonUtils.getInputFilePath(args,"/flight-data/parquet/2010-summary.parquet"))

    parquetFile.write.format("parquet")
      .mode("overwrite")
      .save(CommonUtils.getOutputFilePath(args,"/parquet/my-parquet-file.parquet"))

    spark.stop()
  }
}
