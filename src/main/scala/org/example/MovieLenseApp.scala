package org.example

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object MovieLenseApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("movierelense")

    val master = config.getString("master")
    val appName = config.getString("appName")
    val topRecords = config.getInt("topRecords")

    val moviesDataPath = config.getString("moviesDataPath")
    val ratingsDataPath = config.getString("ratingsDataPath")
    val separator = config.getString("separator")

    val spark = SparkSession.builder().master(master).appName(appName)
      .getOrCreate()

    val moviesSchema = StructType(
      List(
        StructField("MovieID", IntegerType, true),
        StructField("Title", StringType, true),
        StructField("Genres", StringType, true)
      )
    )

    val ratingsSchema = StructType(
      List(
        StructField("UserID", IntegerType, true),
        StructField("MovieID", IntegerType, true),
        StructField("Rating", IntegerType, true),
        StructField("Timestamp", IntegerType, true)
      )
    )

    val moviesDF = spark.read.format("csv").schema(moviesSchema).option("sep", separator).load(moviesDataPath);
    println("Base Movies Data")
    moviesDF.show()

    val ratingsDF = spark.read.format("csv").schema(ratingsSchema).option("sep", separator).load(ratingsDataPath);

    println("Base Ratings Data")
    ratingsDF.show()

    val movieRatingsDF = ratingsDF.groupBy("MovieID").
      agg(
        min("Rating").as("min_rating"),
        max("Rating").as("max_rating"),
        avg("Rating").as("avg_rating")
      )

    println("Movies Data with Min, Max and Average ratings")
    movieRatingsDF.show()


    val ratingsByUserSpec = Window.partitionBy("UserID").orderBy(desc("Rating"))
    val rowNumberColumn = "row_number"
    val userTopRatingsDF = ratingsDF.withColumn(rowNumberColumn, row_number().over(ratingsByUserSpec)).
      filter(s"$rowNumberColumn <= $topRecords")

    println(s"Top rated($topRecords) Movies for User")
    userTopRatingsDF.show()
  }
}
