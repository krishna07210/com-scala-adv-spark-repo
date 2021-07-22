package com.spark.components.DataFrame.Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JoinsOnComplexTypes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JoinsOnComplexTypes")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    import spark.implicits._
    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100))
    ).toDF("id", "name", "graduate_program", "spark_status")
    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")
    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")
    person.show()
    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")
    import org.apache.spark.sql.functions.expr

    person.withColumnRenamed("id", "personId")
      .join(sparkStatus, expr("array_contains(spark_status,id)")).show(false)
    spark.sql(
      """SELECT * FROM
        |(select id as personId, name, graduate_program, spark_status FROM person)
        |INNER JOIN sparkStatus ON array_contains(spark_status, id)""".stripMargin).show(false)

    println("*** 1 -> Handling Duplicate Column Names ***")
    val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
    val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
      "graduate_program")
    person.join(gradProgramDupe, joinExpr).show()
    //    person.join(gradProgramDupe, joinExpr).select("graduate_program").show() ---'graduate_program' is ambiguous

    println("*** Approach 1: Different join expression ***")
    person.join(gradProgramDupe, "graduate_program").select("graduate_program").show()

    println("*** Approach 2: Dropping the column after the join ***")
    person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
      .select("graduate_program").show()
    val joinExpr2 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr2).drop(graduateProgram.col("id")).show()

    println("*** Approach 3: Renaming a column before the join ****")
    val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
    val joinExpr3 = person.col("graduate_program") === gradProgram3.col("grad_id")
    person.join(gradProgram3, joinExpr3).show()

    spark.stop()
  }
}
