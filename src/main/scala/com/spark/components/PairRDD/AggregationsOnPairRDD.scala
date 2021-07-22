package com.spark.components.PairRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AggregationsOnPairRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AggregationsOnPairRDD")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val words = spark.sparkContext
      .parallelize("Spark The Definitive Guide: Big Data Processing Made Simple".split(" "),
        2)
    val chars = words.flatMap(word => word.toLowerCase.toSeq)
    val KVcharacters = chars.map(letter => (letter, 1))

    def maxFunc(left: Int, right: Int): Int = math.max(left, right)

    def addFunc(left: Int, right: Int): Int = left + right

    val nums = spark.sparkContext.parallelize(1 to 30, 5)

    println("*** 1 -> countByKey **")
    val timeout = 1000L
    val confidence = 0.95
    println(KVcharacters.countByKey())
    println(KVcharacters.countByKeyApprox(timeout, confidence))

    println("*** 2 -> groupByKey **")
    KVcharacters.groupByKey().map(row => (row._1,row._2.reduce(addFunc))).collect().foreach(println)

    println("*** 3 -> reduceByKey ** ")
    KVcharacters.reduceByKey(addFunc).collect().foreach(println)

    println("*** 4 -> aggregate ** ")
    val aggregates = nums.aggregate(0)(maxFunc,addFunc)
    println(aggregates)
    val depth = 3
    nums.treeAggregate(0)(maxFunc,addFunc,depth)

    println("*** 5 -> aggregateByKey ** ")
    KVcharacters.aggregateByKey(0)(addFunc,maxFunc).collect()

    println("*** 6 -> combineByKey ** ")
    // in Scala
    val valToCombiner = (value:Int) => List(value)
    val mergeValuesFunc = (vals:List[Int], valToAppend:Int) => valToAppend :: vals
    val mergeCombinerFunc = (vals1:List[Int], vals2:List[Int]) => vals1 ::: vals2
    // now we define these as function variables
    val outputPartitions = 6
    KVcharacters
      .combineByKey(
        valToCombiner,
        mergeValuesFunc,
        mergeCombinerFunc,
        outputPartitions)
      .collect()

    println("*** 7 -> foldByKey ** ")
    KVcharacters.foldByKey(0)(addFunc).collect()

    println("*** 8 -> CoGroups ** ")
    import scala.util.Random
    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
    val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
    val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
    val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))
    charRDD.cogroup(charRDD2, charRDD3).take(5)
    spark.stop()
  }
}
