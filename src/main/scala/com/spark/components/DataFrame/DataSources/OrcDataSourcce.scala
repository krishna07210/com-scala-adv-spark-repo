package com.spark.components.DataFrame.DataSources

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object OrcDataSourcce {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OrcDataSourcce")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val orcFile = spark.read.format("orc")
      .load(CommonUtils.getInputFilePath(args,"/flight-data/orc/2010-summary.orc"))

    orcFile.write.format("orc")
      .mode("overwrite")
      .save(CommonUtils.getOutputFilePath(args,"/orc/flight-data/orc/2010-summary.orc"))

    spark.stop()
  }
}
