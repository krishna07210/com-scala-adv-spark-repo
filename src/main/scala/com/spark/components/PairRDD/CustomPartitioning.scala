package com.spark.components.PairRDD

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CustomPartitioning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CustomPartitioning")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(CommonUtils.getInputFilePath(args,"/retail-data/all/"))
    val rdd = df.coalesce(10).rdd
    df.printSchema()
    import org.apache.spark.HashPartitioner
    rdd.map(r => r(6)).take(5).foreach(println)
    val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
    keyedRDD.partitionBy(new HashPartitioner(10)).take(10)

    keyedRDD
      .partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
      .take(5)

    spark.stop()
  }
}
