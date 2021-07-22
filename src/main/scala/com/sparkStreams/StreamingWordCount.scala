package com.sparkStreams

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StreamingWordCount extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Streaming Word Count")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    linesDF.printSchema()
    import org.apache.spark.sql.functions.expr

    val countsDF = linesDF.select(expr("explode(split(value,' ')) as word"))
      .groupBy("word")
      .count()

    val wordCountQuery = countsDF.writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir")
      //      .outputMode("update")
      //.outputMode("append") //ql.AnalysisException: Append output mode not supported when there are streaming aggregations on
      .outputMode("complete")
      .start()

    logger.info("Listening to localhost:9999")
    wordCountQuery.awaitTermination()

  }
}
