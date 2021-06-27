package com.spark.dataframe

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrameDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameDemo")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val myRange = spark.range(1000).toDF("Numbers")
    myRange.printSchema()
    //    myRange.show(false)
    val divisBy2 = myRange.where("Numbers%2 =0")
    //    divisBy2.show(10)

    val flightData2015 = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(CommonUtils.getInputFilePath("/flight-data/csv/2015-summary.csv"))
    flightData2015.printSchema()
    flightData2015.take(3)
    flightData2015.sort("count").explain()

    spark.conf.set("spark.sql.shuffle.partitions", "5")
    flightData2015.sort("count").take(3)
    flightData2015.createOrReplaceTempView("flight_data_2015")

    println("---------------------------------------")
    val sqlWay = spark.sql(
      """SELECT DEST_COUNTRY_NAME, COUNT(1)
        FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME """)
    sqlWay.explain()

    val dataFrameWay = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count()
    dataFrameWay.explain()

    sqlWay.show()

    spark.sql("SELECT MAX(COUNT) Max_Count FROM flight_data_2015").show()
    import org.apache.spark.sql.functions.max
    flightData2015.select(max("count")).take(1)
    println("----------max SQL-----------")
    val maxSQL = spark.sql(
      """ SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 5
      """)
    maxSQL.show()
    println("------ DataFrame syntax way ----------")
    import org.apache.spark.sql.functions.desc
    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

  }
}
