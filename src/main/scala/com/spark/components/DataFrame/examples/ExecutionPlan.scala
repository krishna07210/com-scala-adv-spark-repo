package com.spark.components.DataFrame.examples

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ExecutionPlan {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExecutionPlan")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    /*
        val df1 = spark.range(2, 10000000, 2)
        val df2 = spark.range(2, 10000000, 4)
        val step1 = df1.repartition(5)
        val step12 = df2.repartition(6)
        val step2 = step1.selectExpr("id * 5 as id")
        val step3 = step2.join(step12, "id")
        val step4 = step3.selectExpr("sum(id)")
        step4.explain()
        step4.collect()

        */
    spark.read
      .option("header", "true")
      .csv(CommonUtils.getInputFilePath(args,"/retail-data/all/online-retail-dataset.csv"))
      .repartition(2)
      .selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")
      .groupBy("is_glass")
      .count()
      .collect()

    scala.io.StdIn.readLine()

    spark.stop()
  }
}
