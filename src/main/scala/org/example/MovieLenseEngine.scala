package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, desc, max, min, row_number}
import org.example.MovieLenseConstants.{movieIDColumn, ratingsColumn, userIDColumn}

object MovieLenseEngine {

  /*
   getMovieMinMaxAvgRatings returns the optional Dataframe with Min, Max and Average Movie ratings.
   */
  def getMovieMinMaxAvgRatings(moviesDF: DataFrame, ratingsDF: DataFrame):Option[DataFrame] = {
    println("calculating Movie Min, Max, Avg, Ratings")

    if(!moviesDF.columns.contains(movieIDColumn)) {
      println(s"Missing $movieIDColumn column in supplied MoviesDF")
      None
    }
    else if(!ratingsDF.columns.contains(ratingsColumn)) {
      println(s"Missing $ratingsColumn column in supplied RatingsDF")
      None
    }
    else {
      val movieRatingsDF = ratingsDF.groupBy(movieIDColumn).
        agg(
          min(ratingsColumn).as("min_rating"),
          max(ratingsColumn).as("max_rating"),
          avg(ratingsColumn).as("avg_rating")
        )
      val moviesWithRatingsDF = moviesDF.join(movieRatingsDF, movieIDColumn)
      Some(moviesWithRatingsDF)
    }
  }

  /**
   * getUserTopRatedMovies returns optional Top Rated Movies Dataframe for the User
   * */
  def getUserTopRatedMovies(moviesDF: DataFrame, ratingsDF: DataFrame, topRecords: Int):Option[DataFrame] = {
    println("calculating User top rated movies")

    if(!moviesDF.columns.contains(movieIDColumn)) {
      println(s"Missing $movieIDColumn column in supplied MoviesDF")
      None
    }
    else if(!ratingsDF.columns.contains(ratingsColumn) || !ratingsDF.columns.contains(userIDColumn)) {
      println(s"Missing either $ratingsColumn or $userIDColumn column in supplied RatingsDF")
      None
    }
    else {

      val ratingsByUserSpec = Window.partitionBy(userIDColumn).orderBy(desc(ratingsColumn))
      val rowNumberColumn = "row_number"

      val userTopRatingsWithMovieDetailsDF = ratingsDF.join(moviesDF, movieIDColumn).
        withColumn(rowNumberColumn, row_number().over(ratingsByUserSpec)).
        filter(s"$rowNumberColumn <= $topRecords").
        drop(rowNumberColumn)

      val columns = userTopRatingsWithMovieDetailsDF.columns.filter(!_.eq(userIDColumn))

      Some(userTopRatingsWithMovieDetailsDF.select(userIDColumn, columns: _*))
    }
  }
}
