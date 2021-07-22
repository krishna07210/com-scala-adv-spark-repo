package com.spark.components.PairRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Random

object JoinsOnPairRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JoinsOnPairRDD")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val words = spark.sparkContext
      .parallelize("Spark The Definitive Guide: Big Data Processing Made Simple".split(" "),
        2)
    val words1 = spark.sparkContext.parallelize("abcde".split(" "), 2)
    val words2 = spark.sparkContext.parallelize("abcd".split(" "), 2)
    val keyChars1 = words1.flatMap(word => word.toUpperCase.toSeq).distinct
    val keyChars2 = words2.flatMap(word => word.toUpperCase.toSeq).distinct
    println("---- Key Pair 1 ")
    val KVPair1 = keyChars1.map(letter => (letter, 1))
    KVPair1.sortByKey() foreach (println)
    println("---- Key Pair 2 ")
    val KVPair2 = keyChars2.map(c => (c, 2))
    KVPair2.sortByKey() foreach (println)
    val outputPartitions = 10
    println("*** 1 -> Inner Join ***")
    KVPair1.join(KVPair2).count()
    KVPair1.join(KVPair2, outputPartitions).count()

    KVPair1.join(KVPair2).sortByKey().foreach(println)
    println("____________________________")
    KVPair1.join(KVPair2, outputPartitions).foreach(println)

    println("*** 2 -> leftOuterJoin ***")
    KVPair1.leftOuterJoin(KVPair2).sortByKey().foreach(println)

    println("*** 3 -> fullOuterJoin ***")
    KVPair1.fullOuterJoin(KVPair2).sortByKey().foreach(println)

    println("*** 4-> rightOuterJoin ***")
    KVPair1.rightOuterJoin(KVPair2).sortByKey().foreach(println)

    println("*** 5 -> cartesian Join ***")
    KVPair1.cartesian(KVPair2).sortByKey().foreach(println)

    println("*** 6 -> zips ***")
    val numRange = spark.sparkContext.parallelize(0 to 9, 2)
    words.zip(numRange).collect().foreach(println)
    println("***Error: Can only zip RDDs with same number of elements in each partition ***")
    spark.stop()
  }
}
