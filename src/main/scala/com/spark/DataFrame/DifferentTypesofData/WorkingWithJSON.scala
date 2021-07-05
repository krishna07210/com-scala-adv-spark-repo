package com.spark.DataFrame.DifferentTypesofData

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithJSON {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithNullsInData")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("*** get_json_object & json_tuple ***")
    val jsonDF = spark.range(1).selectExpr(
      """
        |'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""".stripMargin)
    jsonDF.printSchema()
    jsonDF.show(false)
    import org.apache.spark.sql.functions.{get_json_object, json_tuple, col}
    jsonDF.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey")
    ).show(3,false)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath("/retail-data/by-day/2010-12-01.csv"))
    df.printSchema()
    import org.apache.spark.sql.functions.{struct,to_json}
    df.createOrReplaceTempView("dfTable")
    println(" *** StructType into a JSON string  ***")
    df.select(struct("Description","InvoiceNo").alias("struct_type"))
      .select(to_json(col("struct_type"))).show(3,false)
  }
}
