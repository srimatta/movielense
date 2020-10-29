package org.example

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * MovieLenseApp is the EntryPoint for the Application.
 * Calculates the Minimum, Maximum and Average Ratings for the given Movies
 * And Also calculates the Top rated movies for the User.
 * */

object MovieLenseApp {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val configFilePath = "application.conf"

    val config = ConfigFactory.parseFile(new File(configFilePath)).getConfig("movielense")

    val master = config.getString("master")
    val appName = config.getString("appName")
    val topRatedMovies = config.getInt("topRatedMovies")

    val movieConfig = config.getConfig("movies")
    val ratingsConfig = config.getConfig("ratings")

    val spark = SparkSession.builder().master(master).appName(appName)
      .getOrCreate()


    val moviesDF = MovieLenseData.getDataFrame(spark, movieConfig, SchemaObjects.moviesSchema, MovieLenseUtils.getMovieDataConditionExpr())
    println("Movies Data")
    moviesDF.show()


    val ratingsDF = MovieLenseData.getDataFrame(spark, ratingsConfig, SchemaObjects.ratingsSchema, MovieLenseUtils.getRatingsDataConditionExpr())
    println("Ratings Data")
    ratingsDF.show()

    val moviesWithRatingsDF = MovieLenseEngine.getMovieMinMaxAvgRatings(moviesDF, ratingsDF)
    MovieLenseUtils.printOptionDataFrame(moviesWithRatingsDF, "Movies with Min, Max ,Average Ratings")

    val userTopRatingsDF = MovieLenseEngine.getUserTopRatedMovies(moviesDF, ratingsDF, topRatedMovies)
    MovieLenseUtils.printOptionDataFrame(userTopRatingsDF, s"User $topRatedMovies Top rated movies ")

  }
}
