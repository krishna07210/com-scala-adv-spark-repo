package com.spark.components.DataFrame.BasicOperations

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object BasicStructuredOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BasicStructuredOperations")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    spark.range(2).toDF().collect() // Return array res5: Array[org.apache.spark.sql.Row] = Array([0], [1])

    // Schema Operations
    val df1 = spark.read.format("json")
      .load(CommonUtils.getInputFilePath(args,"/flight-data/json/2015-summary.json"))
    df1.printSchema()

    val schema = spark.read.format("json")
      .load(CommonUtils.getInputFilePath(args,"/flight-data/json/2015-summary.json")).schema
    println(schema)

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false,
        Metadata.fromJson("{\"hello\":\"world\"}"))
    ))
    val df = spark.read.format("json").schema(myManualSchema)
      .load(CommonUtils.getInputFilePath(args,"/flight-data/json/2015-summary.json"))
    df.show(false)

    val columns = spark.read.format("json")
      .load(CommonUtils.getInputFilePath(args,"/flight-data/json/2015-summary.json"))
      .columns
    columns.foreach(println)

    //Creating Rows
    import org.apache.spark.sql.Row
    val myRow = Row("Hello", null, 1, false)
    println("myRow(0) -> " + myRow(0))
    println("myRow(0).asInstanceOf -> " + myRow(0).asInstanceOf[String])
    println("myRow.getString(0) -> " + myRow.getString(0))
    println("myRow.getInt(2) -> " + myRow.getInt(2))
    println("myRow(2) -> " + myRow(2))

    spark.stop()
  }
}
