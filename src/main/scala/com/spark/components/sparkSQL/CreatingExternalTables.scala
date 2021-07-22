package com.spark.components.sparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CreatingExternalTables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CreatingExternalTables")
      .master("local[5]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    spark.sql(
      """ CREATE EXTERNAL TABLE hive_flights (
        | DEST_COUNTRY_NAME STRING,
        | ORIGIN_COUNTRY_NAME STRING,
        | count LONG )
        | ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'D:/Git_krishna07210/com-scala-adv-spark-repo/src/main/resources/srcData/flight-data-hive/'
        |""".stripMargin)

    spark.sql("""select * from hive_flights""").show(3)

    spark.stop()
  }
}
