package com.spark.components.DataFrame.Aggregations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UserDefinedAggregationFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UserDefinedAggregationFunctions")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ba = new BoolAnd
    spark.udf.register("booland", ba)
    import org.apache.spark.sql.functions._
    spark.range(1)
      .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
      .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
      .select(ba(col("t")), expr("booland(f)"))
      .show()

    spark.stop()
  }
}
