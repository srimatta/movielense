package org.example

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SchemaObjects {

  val moviesSchema: StructType = StructType(
    List(
      StructField("MovieID", IntegerType, false),
      StructField("Title", StringType, true),
      StructField("Genres", StringType, true)
    )
  )

  val ratingsSchema: StructType = StructType(
    List(
      StructField("UserID", IntegerType, false),
      StructField("MovieID", IntegerType, false),
      StructField("Rating", IntegerType, false),
      StructField("Timestamp", IntegerType, true)
    )
  )
}
