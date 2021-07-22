package com.spark.components.RDDs

import com.config.CommonUtils
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Programmatically Specifying Schema.
 */
object SchemaRDD {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Programmatically Specifying Schema")
      .master("local[3]")
      .getOrCreate()

    val empRDD = spark.sparkContext.textFile(CommonUtils.getInputFilePath(args,"/textFiles/emp.txt"))
    empRDD.foreach(println)
    val employeeSchema = StructType(Array(
      StructField("empno", StringType, true),
      StructField("ename", StringType, true),
      StructField("job", StringType, true),
      StructField("mgr", StringType, true),
      StructField("hiredate", StringType, true),
      StructField("sal", DoubleType, true),
      StructField("comm", DoubleType, true),
      StructField("deptno", IntegerType, true)
    ))

    val empRowRDD = empRDD.map(_.split(","))
      .map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5).toDouble,
        (if (p(6) == null || p(6) == "") "0" else p(6)).toDouble, // Comm is blank so while converting it gives error
        p(7).toInt))

    //val employeeSchemaRDD1 = spark.sqlContext.applySchema(rowRDD,employeeSchema) -- Deprecated
    val employeeSchemaDF = spark.createDataFrame(empRowRDD, employeeSchema)
    //        .na.fill(0)
    employeeSchemaDF.printSchema()
    employeeSchemaDF.show(false)

    val deptSchema = StructType(Array(
      StructField("deptno", IntegerType, true),
      StructField("dname", StringType, true),
      StructField("location", StringType, true)
    ))

    val deptRDD = spark.sparkContext.textFile(CommonUtils.getInputFilePath(args,"/textFiles/dept.txt"))
    deptRDD.foreach(println)
    val deptRowRDD = deptRDD.map(_.split(",")).map(
      p => Row(p(0).toInt, p(1), p(2))
    )
    val deptSchemaDF = spark.createDataFrame(deptRowRDD, deptSchema)
    deptSchemaDF.printSchema()
    deptSchemaDF.show(false)
    spark.stop()
  }
}
