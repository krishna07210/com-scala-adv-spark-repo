package com.spark.components.DataFrame.Joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithJoinTypes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithJoinTypes")
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

    println("*** 1 -> Inner Joins   ****")
    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    var joinTypeInner = "inner"
    person.join(graduateProgram, joinExpression, joinTypeInner).show(false)
    spark.sql(
      """SELECT * FROM person INNER JOIN graduateProgram
        |ON person.graduate_program = graduateProgram.id""".stripMargin).show()
    spark.sql(
      """SELECT * FROM person JOIN graduateProgram
        |ON person.graduate_program = graduateProgram.id""".stripMargin).show()

    println("*** 2 -> Outer Joins ***")
    val joinTypeOuter = "outer"
    person.join(graduateProgram, joinExpression, joinTypeOuter).show(false)
    spark.sql(
      """SELECT * FROM person FULL OUTER JOIN graduateProgram
        |ON graduate_program = graduateProgram.id""".stripMargin).show()

    println("*** 3 -> Left Outer Joins  ***")
    val joinTypeLeftOuter = "left_outer"
    person.join(graduateProgram, joinExpression, joinTypeLeftOuter).show(false)
    spark.sql("""SELECT * FROM graduateProgram LEFT OUTER JOIN person
                |ON person.graduate_program = graduateProgram.id""".stripMargin).show(false)

    println("*** 4 -> Right Outer Joins ***")
    val joinTypeRightOuter ="right_outer"
    person.join(graduateProgram,joinExpression,joinTypeRightOuter).show(false)
    spark.sql("""SELECT * FROM person RIGHT OUTER JOIN graduateProgram
                |ON person.graduate_program = graduateProgram.id""".stripMargin).show(false)

    println("*** 5 -> Left Semi Joins ***")
    val joinTypeLeftSemi = "left_semi"
    val gradProgram2 = graduateProgram.union(Seq(
      (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
    gradProgram2.createOrReplaceTempView("gradProgram2")
    graduateProgram.join(person, joinExpression, joinTypeLeftSemi).show()

    spark.sql("""SELECT * FROM gradProgram2 LEFT SEMI JOIN person
                |ON gradProgram2.id = person.graduate_program""".stripMargin).show()

    println("*** 6 -> Left Anti Joins ***")
    val joinTypeLeftAnti = "left_anti"
    graduateProgram.join(person, joinExpression, joinTypeLeftAnti).show()
    spark.sql("""SELECT * FROM graduateProgram LEFT ANTI JOIN person
                |ON graduateProgram.id = person.graduate_program""".stripMargin).show()

    println("*** 7 -> Cross (Cartesian) Joins  ")
    val joinTypeCross = "cross"
    graduateProgram.join(person, joinExpression, joinTypeCross).show()
    spark.sql("""SELECT * FROM graduateProgram CROSS JOIN person
                |ON graduateProgram.id = person.graduate_program""".stripMargin).show()
    person.crossJoin(graduateProgram).show()
    spark.sql("""SELECT * FROM graduateProgram CROSS JOIN person""").show()
    spark.stop()
  }
}
