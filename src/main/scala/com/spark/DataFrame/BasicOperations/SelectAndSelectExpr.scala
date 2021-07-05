package com.spark.DataFrame.BasicOperations

import com.config.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SelectAndSelectExpr {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SelectAndSelectExpr")
      .master("local[3]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.format("json")
      .load(CommonUtils.getInputFilePath("/flight-data/json/2015-summary.json"))
    //df.show(false)
    //|DEST_COUNTRY_NAME       |ORIGIN_COUNTRY_NAME|count

    df.select("DEST_COUNTRY_NAME").show(2, false)
    // -- in SQL  SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2
    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(3)

    import org.apache.spark.sql.functions.{col, column, expr}

    df.select(
      //      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      //      'DEST_COUNTRY_NAME,
      //      $"DEST_COUNTRY_NAME",
      //      expr("DEST_COUNTRY_NAME")
    ).show(2)

    df.select(expr("DEST_COUNTRY_NAME as DEST_NAME")).alias("DEST_COUNTRY_NAME").show(1)
    df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(1)

    df.selectExpr("*",
      "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
      .show(2)
    df.selectExpr("avg(count)", "count(distinct (DEST_COUNTRY_NAME))").show()
    println("********** Converting to Spark Types (Literals) **********")
    import org.apache.spark.sql.functions.lit
    df.select(expr("*"), lit(1).as("One")).show(4)
    //Adding Columns
    df.withColumn("numberOne", lit(1)).show(2)
    df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(4)

    df.withColumnRenamed("ORIGIN_COUNTRY_NAME", "COUNTRY_NAME").columns

    println("******** Reserved Characters and Keywords ********")
    val dfWithLongColName =
      df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
    dfWithLongColName.show(2)
    dfWithLongColName.selectExpr(
      "`This Long Column-Name`",
      "`This Long Column-Name` as `new col`").show(2)

    println("******* Removing Columns ********")
    df.drop("count")
    df.show(2)
    df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
    df.select("ORIGIN_COUNTRY_NAME").show(2)

    println("****** Changing a Column’s Type (cast)  *******")
    df.printSchema()
    df.withColumn("count2", col("count").cast("long"))
    df.printSchema()
    println("************ Filtering Rows  *********  ")
    df.filter(col("count") < 2).show(2)
    df.where("count < 2").show(2)
    df.where(col("count") < 2)
      .where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)

    println("************  Getting Unique Rows  ***********")
    val count1 = df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
    println(count1)
    val count2 = df.select("ORIGIN_COUNTRY_NAME").distinct().count()
    println(count2)

    println("**********  Random Samples  ************")
    val seed = 5
    val withReplacement = false
    val fraction = 0.5
    val count3 = df.sample(withReplacement, fraction, seed).count()
    println(count3)

    println("***********  Random Splits  ************")
    val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
    println(df.count())
    println(dataFrames(0).count())
    println(dataFrames(1).count())
    dataFrames(0).count() > dataFrames(1).count()

    println("*************  Concatenating and Appending Rows (Union)  *********")
    import org.apache.spark.sql.Row
    val schema = df.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )
    val parallelizedRows = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(parallelizedRows, schema)
    df.union(newDF)
      .where("count = 1")
      .where(col("ORIGIN_COUNTRY_NAME") =!= "United States")
      .show() // get all of them and we'll see our new rows at the end
  }
}
