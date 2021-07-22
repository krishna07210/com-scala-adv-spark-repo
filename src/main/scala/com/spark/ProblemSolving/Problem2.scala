package com.spark.ProblemSolving

import com.caseclasses.{User, UserActivity}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Problem 2:  Identify the Output for the below code
 */

object Problem2 {
  val LoginActivityTypeId = 0
  val LogoutActivityTypeId = 1

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Problem2")
      .master("local[5]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    readUserData(spark).foreach(println)
    readUserActivity(spark).foreach(println)

    val userRdd: RDD[(Long, User)] = readUserData(spark).map(u => (u.userId, u))
    val userActivityRdd: RDD[(Long, UserActivity)] =
      readUserActivity(spark)
        .map(ua => (ua.userId, ua))

    val result = userRdd.leftOuterJoin(userActivityRdd)
      .filter(e => e._2._2.isDefined && e._2._2.get.activityTypeId == LoginActivityTypeId)
      .map(e => (e._2._1.userName, e._2._2.get.timestampEpochSec))
      .reduceByKey((a, b) => if (a < b) a else b)

    result.foreach(e => println(s"${e._1} : ${e._2}"))

    spark.stop()
  }

  private def readUserData(sparkSession: SparkSession): RDD[User] = {
    sparkSession.sparkContext.parallelize(Array(
      User(1, "Doe, John"),
      User(2, "Doe, Jane"),
      User(3, "X, Mr.")))
  }

  private def readUserActivity(sparkSession: SparkSession): RDD[UserActivity] = {
    sparkSession.sparkContext.parallelize(
      Array(
        UserActivity(1, LoginActivityTypeId, 1514764800L),
        UserActivity(2, LoginActivityTypeId, 1514808000L),
        UserActivity(1, LogoutActivityTypeId, 1514829600L),
        UserActivity(1, LoginActivityTypeId, 1514894400L))
    )
  }
}

/**
 * The main method, calculate, reads two sets of data. (In the example they are provided from a
 * constant inline data structure that is converted into a distributed dataset using parallelize.)
 * The map applied to each of them transforms them into tuples, each consisting of a userId and the
 * object itself. The userId is used to join the two datasets.
 *
 * The joined dataset is filtered by all users with all their login activities.
 * It is then transformed into a tuple consisting of userName and the event timestamp.
 *
 * This is finally reduced to only the first login entry per user and written to the console.
 */