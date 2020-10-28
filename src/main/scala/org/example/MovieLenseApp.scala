package org.example

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{min, max , avg, row_number, desc, col}

object MovieLenseApp {

  def calculateMovieMinMaxAvgRatings(moviesDF: DataFrame, ratingsDF: DataFrame) = {

    val movieRatingsDF = ratingsDF.groupBy("MovieID").
      agg(
        min("Rating").as("min_rating"),
        max("Rating").as("max_rating"),
        avg("Rating").as("avg_rating")
      )

    val moviesWithRatingsDF = moviesDF.join(movieRatingsDF, "MovieID")

    moviesWithRatingsDF

  }

  def calculateUserTopRatedMovies(moviesDF: DataFrame, ratingsDF: DataFrame, topRecords: Int) = {

    val ratingsByUserSpec = Window.partitionBy("UserID").orderBy(desc("Rating"))
    val rowNumberColumn = "row_number"

    val userTopRatingsWithMovieDetailsDF = ratingsDF.join(moviesDF, "MovieID").
      withColumn(rowNumberColumn, row_number().over(ratingsByUserSpec)).
      filter(s"$rowNumberColumn <= $topRecords").
      drop(rowNumberColumn)

    val columns = userTopRatingsWithMovieDetailsDF.columns.filter(!_.eq("UserID"))

    userTopRatingsWithMovieDetailsDF.select("UserID", columns: _*)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("movierelense")

    val master = config.getString("master")
    val appName = config.getString("appName")
    val topRecords = config.getInt("topRecords")

    val moviesDataPath = config.getString("moviesDataPath")
    val ratingsDataPath = config.getString("ratingsDataPath")
    val dataFormat = config.getString("dataFormat")
    val separator = config.getString("separator")

    val spark = SparkSession.builder().master(master).appName(appName)
      .getOrCreate()

    val moviesSchema = SchemaObjects.moviesSchema
    val ratingsSchema = SchemaObjects.ratingsSchema

    val moviesDF = spark.read.format(dataFormat).schema(moviesSchema).option("sep", separator).load(moviesDataPath);
    println("Base Movies Data")
    moviesDF.show()

    val ratingsDF = spark.read.format(dataFormat).schema(ratingsSchema).option("sep", separator).load(ratingsDataPath);

    println("Base Ratings Data")
    ratingsDF.show()

    val moviesWithRatingsDF = calculateMovieMinMaxAvgRatings(moviesDF, ratingsDF)

    val userTopRatingsDF = calculateUserTopRatedMovies(moviesDF, ratingsDF, topRecords)

    println("Movies Data with Min, Max and Average ratings")
    moviesWithRatingsDF.show()

    println(s"Top $topRecords rated Movies for User")
    userTopRatingsDF.show()
  }
}
