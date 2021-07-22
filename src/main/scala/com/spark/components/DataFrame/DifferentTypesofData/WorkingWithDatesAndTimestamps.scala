package com.spark.components.DataFrame.DifferentTypesofData

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorkingWithDatesAndTimestamps {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WorkingWithDatesAndTimestamps")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    import org.apache.spark.sql.functions.{current_date, current_timestamp}
    println("******** current_date() and current_timestamp() ")
    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
    dateDF.createOrReplaceTempView("dateTable")
    dateDF.printSchema()
    println("******* date_add()  &  date_sub() ")
    import org.apache.spark.sql.functions.{col, date_add, date_sub}
    dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
    spark.sql("""select date_sub(today,5) , date_add(today,5) from dateTable""").show(2)
    println("*******  datediff and months_between")
    import org.apache.spark.sql.functions.{datediff, lit, months_between, to_date}
    dateDF.withColumn("weekago", date_sub(current_date(), 7))
      .select(datediff(col("weekago"), col("today"))).show(1)
    dateDF.select(
      to_date(lit("2016-01-01")).alias("start"),
      to_date(lit("2017-05-22")).alias("end")
    ).select(months_between(col("start"), col("end"))).show(1)

    spark.sql(
      """SELECT to_date('2016-01-01'), months_between('2016-01-01', '2017-01-01'),
        |datediff('2016-01-01', '2017-01-01')
        |FROM dateTable""".stripMargin).show()
    println("******* DateFormat ********")
    val dateFormat = "yyyy-dd-MM"
    val cleanDateDF = spark.range(1)
      .select(
        to_date(lit("2017-12-11"), dateFormat).alias("date"),
        to_date(lit("2017-20-12"), dateFormat).alias("date2")
      )
    cleanDateDF.createOrReplaceTempView("dateTable2")

    spark.sql(
      """select to_timestamp(date,'yyyy-dd-MM'), to_timestamp(date2,'yyyy-dd-MM')
        | from dateTable2
        |""".stripMargin).show()
     spark.sql("""SELECT cast(to_date("2017-01-01", "yyyy-dd-MM") as timestamp)""")
    spark.stop()
  }
}
