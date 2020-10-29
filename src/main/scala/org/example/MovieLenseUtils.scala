package org.example

import org.apache.spark.sql.DataFrame

object MovieLenseUtils {

  def printOptionDataFrame(optDF : Option[DataFrame], msg : String = ""): Unit = {
    optDF match {
      case Some(df) =>
        println(msg)
        df.show()
      case None => println("Nothing to show.")
    }
  }

  def getMovieDataConditionExpr():String = {
    val movieDataConditionExpr = s"${MovieLenseConstants.movieIDColumn} is not null"
    movieDataConditionExpr
  }

  def getRatingsDataConditionExpr():String = {
    val ratingsDataConditionExpr = s"${MovieLenseConstants.userIDColumn} is not null " +
      s"AND ${MovieLenseConstants.ratingsColumn} is not null " +
      s"AND ${MovieLenseConstants.movieIDColumn} is not null"
    ratingsDataConditionExpr
  }

}
