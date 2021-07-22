package com.spark.components.DataFrame.DataSources

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WritingToSQLDatabases {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WritingToSQLDatabases")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val driver = "org.sqlite.JDBC"
    val path = "/data/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:/${path}"
    val tableName = "flight_info"

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

    val props = new java.util.Properties
    props.setProperty("driver", "org.sqlite.JDBC")
    val newPath = "jdbc:sqlite://tmp/my-sqlite.db"
    csvFile.write.mode("overwrite").jdbc(newPath, tableName, props)

    spark.read.jdbc(newPath, tableName, props).count() // 255
    csvFile.write.mode("append").jdbc(newPath, tableName, props)
    spark.read.jdbc(newPath, tableName, props).count() // 765

    spark.stop()
  }
}
