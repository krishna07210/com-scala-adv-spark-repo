package com.spark.components.sparkSQL

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CreatingTables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CreatingTables")
      .master("local[5]")
//      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    spark.sql("""drop table flights""")
    spark.sql(
      """CREATE TABLE flights (
        |DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
        |USING JSON OPTIONS (path 'D:/Git_krishna07210/com-scala-adv-spark-repo/src/main/resources/srcData/flight-data/json/2015-summary.json')""".stripMargin)

    spark.sql("""select * from flights""").show(false)
    spark.read.json(CommonUtils.getInputFilePath(args,("/flight-data/json/2015-summary.json")))
      .createOrReplaceTempView("some_sql_view")

    val cnt = spark.sql(
      """ select DEST_COUNTRY_NAME , sum(count)
        |FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
        |""".stripMargin)
      .where("DEST_COUNTRY_NAME like 'S%' ")
      .where("`sum(count)` > 10 ")
      .count()

    println("Count - " + cnt)

    spark.sql("""drop table flights_csv""")

    spark.sql(
      """ CREATE TABLE flights_csv (
        | DEST_COUNTRY_NAME  STRING,
        | ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
        | COUNT LONG)
        | USING csv OPTIONS(header true, path 'D:/Git_krishna07210/com-scala-adv-spark-repo/src/main/resources/srcData/flight-data/csv/2015-summary.csv')
        |""".stripMargin)

    spark.sql("""select * from flights_csv """).show(4, false)

    spark.sql("""drop table flights_from_select""")

    spark.sql(
      """create table flights_from_select USING parquet
        |AS
        |SELECT * FROM flights
        |""".stripMargin)

    spark.sql("""select * from flights_from_select """).show(5, false)

    spark.sql(
      """CREATE TABLE IF NOT EXISTS flights_from_select
        |as
        |select * from flights
        |""".stripMargin)
    spark.sql("""select * from flights_from_select """).show(6, false)

    println("*** partitioned dataset ***")
    spark.sql(
      """CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
        |AS
        |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5
        |""".stripMargin)
    spark.sql("select * from partitioned_flights ").show(false)
    spark.stop()
  }
}
