package org.spark.handson.utilities

import org.apache.spark.sql.SparkSession

object SparkConnection extends Logging {

  def getConnection( appName: String, master: String): SparkSession = {
    val sparkSession = SparkSession.builder()
                        .appName(appName)
                        .master(master).getOrCreate()

    logger.info("Initialized Spark Session.....")
    sparkSession
  }
}
