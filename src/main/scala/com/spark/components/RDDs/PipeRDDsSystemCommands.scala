package com.spark.components.RDDs

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * The pipe method is probably one of Spark’s more interesting methods. With pipe, you can
 * return an RDD created by piping elements to a forked external process. The resulting RDD is
 * computed by executing the given process once per partition. All elements of each input partition
 * are written to a process’s stdin as lines of input separated by a newline. The resulting partition
 * consists of the process’s stdout output, with each line of stdout resulting in one element of the
 * output partition. A process is invoked even for empty partitions.
 */

object PipeRDDsSystemCommands {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PipeRDDsSystemCommands")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val text: String = "Spark The Definitive Guide : Big Data Processing Made Simple"
    val words = spark.sparkContext.parallelize(text.split(" "), 3)
    //    words.pipe("wc -l").collect().foreach(println)

    println("** 1-> mapPartitions")
    println(words.mapPartitions(part => Iterator[Int](1)).sum())
    println("** 2-> mapPartitionsWithIndex")
    words.mapPartitionsWithIndex(indexedFunc).collect()
    println("** 3-> foreachPartition")

    words.foreachPartition { iter =>
      import java.io._
      import scala.util.Random
      val randomFileName = new Random().nextInt()
      val pw = new
          PrintWriter(new File(CommonUtils.getOutputFilePath(args,s"/random-file-${randomFileName}.txt")))
      while (iter.hasNext) {
        pw.write(iter.next())
      }
      pw.close()
    }
    println("** 2-> glom ")
    spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect()

    spark.stop()
  }

  def indexedFunc(partitionIndex: Int, withinPartIterator: Iterator[String]) = {
    withinPartIterator.toList.map(
      value => s"Partition: $partitionIndex => $value").iterator
  }
}
