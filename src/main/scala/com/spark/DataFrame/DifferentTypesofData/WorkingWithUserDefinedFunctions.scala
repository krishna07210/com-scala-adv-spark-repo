package com.spark.DataFrame.DifferentTypesofData

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithUserDefinedFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithNullsInData")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

  }
}
