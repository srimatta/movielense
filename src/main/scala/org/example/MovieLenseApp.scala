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

    val movies_df = spark.read.format("csv").schema(moviesSchema).option("sep", separator).load(moviesDataPath);

    movies_df.show()

    val ratings_df = spark.read.format("csv").schema(ratingsSchema).option("sep", separator).load(ratingsDataPath);

    ratings_df.show()

    val ratings_min_max_df = ratings_df.groupBy("MovieID").
      agg(
        min("Rating").as("min_rating"),
        max("Rating").as("max_rating"),
        avg("Rating").as("avg_rating")
      )

    ratings_min_max_df.show()

    val joined_df = movies_df.join(ratings_min_max_df, movies_df("MovieID") === ratings_min_max_df("MovieID"))
    joined_df.show()

    val ratingsByUserSpec = Window.partitionBy("UserID").orderBy(desc("Rating"))
    val rowNumberColumn = "row_number"
    val user_top_ratings_df = ratings_df.withColumn(rowNumberColumn, row_number().over(ratingsByUserSpec)).
      filter(s"$rowNumberColumn <= $topRecords")

    user_top_ratings_df.show()
  }
}
