package com.spark.components.DataFrame

import com.caseclasses.Flight
import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

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
      .csv(CommonUtils.getInputFilePath(args, "/flight-data/csv/2015-summary.csv"))
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

    println("***** Dataset ******")
    val flightDF = spark.read
      .parquet(CommonUtils
        .getInputFilePath(args, "/flight-data/parquet/2010-summary.parquet/"))

    import spark.implicits._
    val flights: Dataset[Flight] = flightDF.as[Flight]
    //or  ->   val flights = flightDF.as[Flight]
    flights.filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)

    println("***** Structured Streaming ******")

    val staticDataframe = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath(args, "/retail-data/by-day/*.csv"))
    staticDataframe.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataframe.schema

    import org.apache.spark.sql.functions.{window, column, desc, col}
    staticDataframe
      .selectExpr("CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5, false)

    /*
      val streamingDataFrame = spark.readStream
        .schema(staticSchema)
        .option("maxFilesPerTrigger", 1)
        .format("csv")
        .option("header", "true")
        .load(CommonUtils.getInputFilePath(args,path,"/retail-data/by-day/*.csv"))
  */

     */
    /**
     * maxFilesPerTrigger : maxFilesPerTrigger option specifies the maximum number of files per
     * trigger (batch). It limits the file stream source to read the maxFilesPerTrigger number
     * of files specified at a time and hence enables rate limiting.
     * It allows for a static set of files be used like a stream for testing as the file set is
     * processed maxFilesPerTrigger number of files at a time.
     */

    // println("Is DataFrame is Streaming :" + streamingDataFrame.isStreaming) // returns true
    /*
        val purchaseByCustomerPerHour = streamingDataFrame
          .selectExpr(
            "CustomerId",
            "(UnitPrice * Quantity) as total_cost",
            "InvoiceDate")
          .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
          .sum("total_cost")
        purchaseByCustomerPerHour.writeStream
          .format("memory") // memory = store in-memory table
          .queryName("customer_purchases") // the name of the in-memory table
          .outputMode("complete") // complete = all the counts should be in the table
          .start()
    */
    spark.stop()
  }
}
