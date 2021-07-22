package com.spark.components.DataFrame.DataSources

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SQLDatabasesSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLDatabasesSource")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("*** Reading from the Database  ***")

    val driver = "org.sqlite.JDBC"
    val path = "/data/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:/${path}"
    val tableName = "flight_info"

    import java.sql.DriverManager
    val connection = DriverManager.getConnection(url)
    connection.isClosed
    connection.close()

    val dbDataFrame = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", tableName)
      .option("driver", driver)
      .load()

    val pgDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://database_server")
      .option("dbtable", "schema.tablename")
      .option("user", "username").option("password", "my-secret-password").load()

    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)
    println("*** Query Pushdown:  First, Spark makes a best-effort attempt to filter data in " +
      "the database itself before creating the DataFrame ***")
    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().explain()

    val pushdownQuery =
      """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info"""
    val dbDataFramePushDown = spark.read.format("jdbc")
      .option("url",url)
      .option("dbtable",pushdownQuery)
      .option("driver",driver)
      .load()

    dbDataFramePushDown.explain()

    spark.stop()
  }
}
