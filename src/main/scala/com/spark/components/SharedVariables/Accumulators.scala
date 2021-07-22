package com.spark.components.SharedVariables

import com.caseclasses.Flight
import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Accumulators {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Accumulators")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._
    val flights = spark.read.parquet(CommonUtils.getInputFilePath(args,"flight-data/parquet/2010-summary.parquet"))
      .as[Flight]

    import org.apache.spark.util.LongAccumulator
    // Unnamed Accumulators
    val accUnnamed = new LongAccumulator
    val acc = spark.sparkContext.register(accUnnamed)

    //Named Accumulators
    val accChina = new LongAccumulator
    val accChina2 = spark.sparkContext.longAccumulator("China")
    spark.sparkContext.register(accChina,"China")


  def accChinaFunc (flight_row : Flight) = {
    val destination = flight_row.DEST_COUNTRY_NAME
    val origin = flight_row.ORIGIN_COUNTRY_NAME

    if (destination == "China") {
      accChina.add(flight_row.count.toLong)
    }
    if (origin =="China"){
      accChina.add(flight_row.count.toLong)
    }
  }

    flights.foreach(flight_row => accChinaFunc(flight_row))
    println("Accumulator -> "+ accChina.value)

    spark.stop()
  }
}
