package com.spark.components.sparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DescribingTableMetadata {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DescribingTableMetadata")
      .master("local[5]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    /*
    spark.sql("""drop table flights """)
    spark.sql(
      """CREATE TABLE flights (
        |DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
        |USING JSON OPTIONS (path 'D:/Git_krishna07210/com-scala-adv-spark-repo/src/main/resources/srcData/flight-data/json/2015-summary.json')""".stripMargin)

    //    spark.sql("""drop table flights_from_select""")
    spark.sql(
      """create table flights_from_select
        |AS
        |SELECT * FROM flights
        |""".stripMargin)

    spark.sql("""select * from flights_from_select """).show(5, false)

     spark.sql(
      """INSERT INTO flights_from_select
        |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20
        |""".stripMargin)

    println("*** provide a partition specification ***")
    /*
        spark.sql(
          """CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
            |AS
            |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5
            |""".stripMargin)
        spark.sql(
          """INSERT INTO partitioned_flights
            |PARTITION (DEST_COUNTRY_NAME="UNITED STATES")
            |SELECT count, ORIGIN_COUNTRY_NAME FROM flights
            |WHERE DEST_COUNTRY_NAME='UNITED STATES' LIMIT 12""".stripMargin)
    */
    spark.sql("""DESCRIBE TABLE flights_from_select""")

     */

    spark.sql("""create database IF NOT EXISTS demo""")
    spark.catalog.listDatabases().show(false)
    spark.catalog.listTables().show()
    spark.catalog.listColumns("default","hive_flights").show(false)
    spark.stop()
  }
}
