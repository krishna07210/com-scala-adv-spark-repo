package com.spark.components.DataFrame.DataSources

import com.sparkStreams.FileStreams.FileStreamDemo.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReadingS3BucketFile {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Reading File from S3 Bucket")
      .master("local[3]")
      .getOrCreate()

    //    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "awsaccesskey value")
    //    // Replace Key with your AWS secret key (You can find this on IAM
    //    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "aws secretkey value")
    //    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val df = spark.read
      .format("csv")
      .option("path", "s3://kisna-spark-bucket-01/RawFiles/flight-data/csv/2010-summary.csv")
      .load()

    df.printSchema()
    df.show()
  }
}
