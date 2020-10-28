package org.example

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SchemaObjects {

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
}
