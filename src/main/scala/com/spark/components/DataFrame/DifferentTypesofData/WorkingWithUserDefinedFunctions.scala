package com.spark.components.DataFrame.DifferentTypesofData

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithUserDefinedFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithUserDefinedFunctions")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val udfExampleDF = spark.range(5).toDF("num")
    udfExampleDF.show()
    import org.apache.spark.sql.functions.{udf,col}
    val power3udf = udf(power3(_:Double):Double)
    udfExampleDF.select(power3udf(col("num"))).show()

    println("*** Register UDF ***")
    spark.udf.register("power3",power3(_:Double):Double)
    udfExampleDF.selectExpr("power3(num)").show(2)
    spark.sql(""" select power3(12) """).show()

    spark.stop()
  }

  def power3(number: Double): Double = number * number * number
}
