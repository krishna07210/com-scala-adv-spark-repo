package com.spark.components.DataFrame.WindowFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RankingFunctions {
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

    /** row_number() window function is used to give the sequential
     * row number starting from 1 to the result of each window partition.
     */
    df.withColumn("row_number", row_number.over(windowSpec))
      .show()

    /** rank() window function is used to provide a rank to the result within a window partition.
     * This function leaves gaps in rank when there are ties.
     */
    df.withColumn("rank", rank().over(windowSpec))
      .show()

    /** dense_rank() window function is used to get the result with rank
     * of rows within a window partition without any gaps
     */
    df.withColumn("dense_rank", dense_rank().over(windowSpec))
      .show()

    /** percent_rank Window Function      */
    df.withColumn("percent_rank", percent_rank.over(windowSpec))
      .show()

    /** ntile() window function returns the relative rank of result rows within a window partition.     */
    df.withColumn("ntile", ntile(2).over(windowSpec))
      .show()
  }
}
