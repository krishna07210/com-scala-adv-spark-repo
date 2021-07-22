package com.spark.components.Datasets

import com.caseclasses.Flight
import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DatasetsDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WritingDataInParallel")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val flightsDF = spark.read
      .parquet(CommonUtils.getInputFilePath(args,"flight-data/parquet/2010-summary.parquet/"))
    import spark.implicits._
    val flights = flightsDF.as[Flight]
    flights.show(false)
    println("flights.first.DEST_COUNTRY_NAME -> "+flights.first.DEST_COUNTRY_NAME)

    spark.stop()
  }
}
