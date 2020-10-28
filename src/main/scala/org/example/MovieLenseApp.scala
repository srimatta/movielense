package org.example

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object MovieLenseApp {


  def printOptionDataFrame(optDF : Option[DataFrame], msg : String = ""): Unit = {
    optDF match {
      case Some(df) =>
        println(msg)
        df.show()
      case None => println("Nothing to show.")
    }
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

    val moviesDF = spark.read.format(dataFormat).schema(moviesSchema).option("sep", separator).load(moviesDataPath)
    println("Base Movies Data")
    moviesDF.show()

    val ratingsDF = spark.read.format(dataFormat).schema(ratingsSchema).option("sep", separator).load(ratingsDataPath)

    println("Base Ratings Data")
    ratingsDF.show()

    val moviesWithRatingsDF = MovieLenseEngine.getMovieMinMaxAvgRatings(moviesDF, ratingsDF)
    printOptionDataFrame(moviesWithRatingsDF, "Movies with Min, Max ,Average Ratings")

    val userTopRatingsDF = MovieLenseEngine.getUserTopRatedMovies(moviesDF, ratingsDF, topRecords)
    printOptionDataFrame(userTopRatingsDF, s"User $topRecords Top rated movies ")

  }
}
