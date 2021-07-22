package com.spark.components.DataFrame.DataSources

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WritingDataInParallel {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WritingDataInParallel")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("*** Partitioning ***")
    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
    val myManualSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))
    val csvFile = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load(CommonUtils.getInputFilePath(args,"/flight-data/csv/2010-summary.csv"))
    csvFile.show(10)

    csvFile.limit(10).write.mode("overwrite")
      .partitionBy("DEST_COUNTRY_NAME")
      .save(CommonUtils.getOutputFilePath(args,"/parquetPart/partitioned-files.parquet"))

    println("*** Bucketing ***")
    val numberBuckets = 10
    val columnToBucketBy = "count"

    csvFile.write.format("parquet")
      .mode("overwrite")
      .bucketBy(numberBuckets,columnToBucketBy)
      .saveAsTable("bucketedFiles")


    csvFile.write.format("parquet")
      .mode("overwrite")
      .bucketBy(numberBuckets,columnToBucketBy)
      .save(CommonUtils.getOutputFilePath(args,"/parquetBucket/bucketed-files.parquet"))
    //'save' does not support bucketBy right now;

    spark.stop()
  }
}
