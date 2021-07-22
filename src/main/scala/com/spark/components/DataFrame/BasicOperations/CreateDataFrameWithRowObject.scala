package com.spark.components.DataFrame.BasicOperations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object CreateDataFrameWithRowObject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BasicStructuredOperations")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val myManualSchema = new StructType(
      Array(
        new StructField("some", StringType, true),
        new StructField("col", StringType, true),
        new StructField("names", LongType, false)
      )
    )
    import org.apache.spark.sql.Row
    val myRows = Seq(Row("Hello",null,1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD,myManualSchema)
    myDf.show()

    import spark.implicits._
    val myDf1 = Seq(("Hello",null,1L)).toDF("some","col","names")
    myDf1.show()

    spark.stop()
  }
}
