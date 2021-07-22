package com.spark.components.Datasets

import com.caseclasses.{Flight, FlightMetadata}
import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DatasetTransformations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetTransformations")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val flightsDF = spark.read
      .parquet(CommonUtils.getInputFilePath(args,"flight-data/parquet/2010-summary.parquet/"))
    import spark.implicits._
    val flights = flightsDF.as[Flight]
    flights.show(false)
//    println("flights.first.DEST_COUNTRY_NAME -> " + flights.first.DEST_COUNTRY_NAME)

    flights.filter(flight_row => originIsDestination(flight_row)).show(false)
    println("*** Mapping ****")
    val destinations = flights.map(f => f.DEST_COUNTRY_NAME)
    destinations.show()

    println("*** Joins ***")
    val flightMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
      .withColumnRenamed("_1", "count")
      .withColumnRenamed("_2", "randomData")
      .as[FlightMetadata]
    flightMeta.show(5)

    val flight2= flights
      .joinWith(flightMeta,flights.col("count")===flightMeta.col("count"))
    flight2.show(false)

    flight2.selectExpr("_1.DEST_COUNTRY_NAME").show(false)

    println("*** Joining a DataFrame and a Dataset ***")
    val flight3 = flights.join(flightMeta.toDF(),Seq("count"))
    flight3.show(false)

    println("*** Grouping and Aggregations ***")
    flights.groupBy("DEST_COUNTRY_NAME").count().show(false)
    flights.groupByKey(x=>x.DEST_COUNTRY_NAME).count().show(false)
    flights.groupByKey(x=>x.DEST_COUNTRY_NAME).count().explain()

    spark.stop()
  }

  def originIsDestination(flight_row: Flight): Boolean = {
    return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
  }
}
