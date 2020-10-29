package org.example

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import matchers._

class MovieLenseEngineSpec extends FunSuite  {

  val config = ConfigFactory.parseFile(new File("src/test/application.conf")).getConfig("movielense")
  val movieConfig = config.getConfig("movies")
  val ratingsConfig = config.getConfig("ratings")
  val spark = SparkSession.builder().master("local").appName("TestApp")
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  test("Test Movie Min Max Average") {
    val moviesDF = MovieLenseData.getDataFrame(spark, movieConfig, SchemaObjects.moviesSchema, MovieLenseUtils.getMovieDataConditionExpr())

    assert(1 < moviesDF.count())
    val ratingsDF = MovieLenseData.getDataFrame(spark, ratingsConfig, SchemaObjects.ratingsSchema, MovieLenseUtils.getRatingsDataConditionExpr())

    assert(1 < ratingsDF.count())

    val moviesWithRatingsDFOption = MovieLenseEngine.getMovieMinMaxAvgRatings(moviesDF, ratingsDF)

    val moviesMinMaxAvgDF  = moviesWithRatingsDFOption.get
    val toyStoryData = moviesMinMaxAvgDF.select("min_rating", "max_rating", "avg_rating").where("MovieID == 1").collect()(0)

    println("toyStoryData :"+ toyStoryData.mkString(","))
    val minRating = toyStoryData(0)
    val maxRating = toyStoryData(1)
    val avgRating = toyStoryData(2)

    assert(minRating == 3)
    assert(maxRating == 5)
    assert(avgRating == 4.0)
 }

  test("Test User Top Rated Movies") {

    val moviesDF = MovieLenseData.getDataFrame(spark, movieConfig, SchemaObjects.moviesSchema, MovieLenseUtils.getMovieDataConditionExpr())

    assert(1 < moviesDF.count())

    val ratingsDF = MovieLenseData.getDataFrame(spark, ratingsConfig, SchemaObjects.ratingsSchema, MovieLenseUtils.getRatingsDataConditionExpr())

    assert(1 < ratingsDF.count())

    val userTopRatingsDF = MovieLenseEngine.getUserTopRatedMovies(moviesDF, ratingsDF, 2)
    val moviesMinMaxAvgDF  = userTopRatingsDF.get
    val userOneData = moviesMinMaxAvgDF.select("UserID", "MovieID", "Rating").where("UserID == 1").limit(1).collect()(0)

    println("userOneData :"+ userOneData.mkString(","))
    val userID = userOneData(0)
    val movieId = userOneData(1)
    val rating = userOneData(2)

    assert(userID == 1)
    assert(movieId == 2)
    assert(rating == 5)

  }
}
