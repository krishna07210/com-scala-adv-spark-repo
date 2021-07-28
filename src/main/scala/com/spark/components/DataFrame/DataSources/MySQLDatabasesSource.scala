package com.spark.components.DataFrame.DataSources

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MySQLDatabasesSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MySQLDatabasesSource")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("*** Reading from the Database  ***")
//
        val jdbcDF = spark.read
          .format("jdbc")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("url", "jdbc:mysql://mysql-testdb.cshdghne8eiu.us-east-2.rds.amazonaws.com")
          .option("dbtable", "retail_db.customers")
          .option("user", "admin")
          .option("password", "Kittu07210")
          .load()

//        val jdbcDF = spark.sqlContext.read.format("jdbc")
//          .option("url", "jdbc:mysql:User=admin;Password=Kittu07210;Database=mysql-testdb;Server=mysql-testdb.cshdghne8eiu.us-east-2.rds.amazonaws.com;Port=3306;")
//          .option("dbtable", "Orders")
//          .option("driver", "cdata.jdbc.mysql.MySQLDriver").load()
        jdbcDF.printSchema()

    spark.stop()
  }
}
