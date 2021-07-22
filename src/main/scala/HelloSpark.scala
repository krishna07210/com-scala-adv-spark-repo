import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HelloSpark {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaSourceForStreamingQueries Demo")
      .master("local[3]")
      .getOrCreate()

    val df = spark.range(100).toDF()
    df.printSchema()
    df.show(10, false)
    spark.stop()
  }
}
