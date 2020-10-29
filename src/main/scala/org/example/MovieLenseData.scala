package org.example

import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object MovieLenseData {

  def getDataFrame(spark: SparkSession, config : Config, schema : StructType, conditionExpr : String ) : DataFrame = {

    val moviesDataPath = config.getString("dataPath")
    val dataFormat = config.getString("dataFormat")
    val separator = config.getString("separator")

    val dataframe  = spark.read.format (dataFormat).schema (schema).option ("sep", separator).load (moviesDataPath).filter(conditionExpr)

    dataframe
  }
}
