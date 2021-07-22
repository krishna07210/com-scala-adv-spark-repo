package com.spark.components.DataFrame.WindowFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowAnalyticFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameDemo")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._
    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()

    val windowSpec = Window.partitionBy("department").orderBy("salary")
    /** cume_dist() window function is used to get the cumulative distribution
     * of values within a window partition.
     */
    df.withColumn("cume_dist",cume_dist().over(windowSpec))
      .show()
    //Lag
    df.withColumn("lag",lag("salary",2).over(windowSpec))
      .withColumn("lead",lead("salary",2).over(windowSpec))
    df.show()
  }
}
