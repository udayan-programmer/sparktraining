package org.spark.handson.utilities

import org.apache.spark.sql.SparkSession

trait SparkSessionConnection extends Logging {
  val spark: SparkSession = SparkConnection.getConnection(appName = "SparkTraining", master = "local[*]")

}
