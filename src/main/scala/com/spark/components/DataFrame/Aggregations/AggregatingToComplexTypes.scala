package com.spark.components.DataFrame.Aggregations

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AggregatingToComplexTypes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AggregatingToComplexTypes")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(CommonUtils.getInputFilePath(args,"/retail-data/all/*.csv"))
      .coalesce(5)
    df.printSchema()
    df.cache()
    df.createOrReplaceTempView("dfTable")
    df.show(3, false)
    import org.apache.spark.sql.functions.{collect_set,collect_list}
    df.agg(collect_set("Country"),
      collect_list("Country")).show()

    spark.sql(
      """select collect_set(Country) ,
        |collect_list(Country) from dfTable
        |""".stripMargin).show()
    spark.stop()
  }
}
