package org.spark.handson.utilities

import org.apache.spark.sql.SparkSession

trait SparkSessionConnection extends Logging {

  def getConnection( appName: String, master: String): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .master(master).getOrCreate()

    logger.info("Initialized Spark Session.....")
    sparkSession
  }

  val spark: SparkSession = getConnection(appName = "SparkTraining", master = "local[*]")

}
