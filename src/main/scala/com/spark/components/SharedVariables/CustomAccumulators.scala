package com.spark.components.SharedVariables

import com.caseclasses.Flight
import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object CustomAccumulators {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CustomAccumulators")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    import spark.implicits._
    val flights = spark.read.parquet(CommonUtils.getInputFilePath(args,"flight-data/parquet/2010-summary.parquet"))
      .as[Flight]

    val arr = ArrayBuffer[BigInt]()
    val acc = new EvenAccumulator
    val newAcc = spark.sparkContext.register(acc, "evenAcc")
    acc.value // 0
    flights.foreach(flight_row => acc.add(flight_row.count))
    println(acc.value) // 31390

    spark.stop()
  }
}
