package com.spark.components.RDDs

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Checkpointing
 * One feature not available in the DataFrame API is the concept of checkpointing. Checkpointing
 * is the act of saving an RDD to disk so that future references to this RDD point to those
 * intermediate partitions on disk rather than recomputing the RDD from its original source. This is
 * similar to caching except that it’s not stored in memory, only disk. This can be helpful when
 * performing iterative computation, similar to the use cases for caching:
 */

object Checkpointing {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Checkpointing")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    spark.sparkContext.setCheckpointDir(CommonUtils.getOutputFilePath(args,"/checkpointing"))
    val text: String = "Spark The Definitive Guide : Big Data Processing Made Simple"
    val words = spark.sparkContext.parallelize(text.split(" "), 2)
    words.checkpoint()
    println(spark.sparkContext.getCheckpointDir)
    spark.stop()
  }
}
