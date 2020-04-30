package org.spark.handson.batch

import java.util.Properties

import org.spark.handson.utilities.SparkSessionConnection

package object dataframe extends SparkSessionConnection {

  // Database connection properties
  val url = "jdbc:mysql://localhost/bigdata"
  val user = "root"
  val password = "Poimnblk00$"
  val driver = "com.mysql.jdbc.Driver"
  val savemode = "Append"

  val dbConnection: Properties = new Properties()
  dbConnection.setProperty("url", url)
  dbConnection.setProperty("user", user)
  dbConnection.setProperty("password", password)
}
