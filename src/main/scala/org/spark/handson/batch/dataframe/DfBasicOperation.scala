package org.spark.handson.batch.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

object DfBasicOperation extends App {

  /**
   *  Here below things will be covered
   *  org.apache.spark.sql.Dataset
   *      Actions,
   *      Functions
   */

  /**
   * Lets create few RDDs and DataFrame to perform operations
   */
  val rddWords =
  spark.sparkContext.parallelize(Seq("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"))

  val dfParquetFile = spark.read.parquet("src/main/resources/input/parquet")

  /**
   * org.apache.spark.sql.Dataset (Class)
   * Actions:
   * count, foreach, foreachPartition, show
   */
  // Count number of records in DF
  logger.info("Total number of records " + dfParquetFile.count())

  dfParquetFile.printSchema()

  dfParquetFile.foreach(row => {
    println(row.getInt(6) + " " + row.getString(7))
  })

  dfParquetFile.foreachPartition(partition => {
    while (partition.hasNext){
      val line: Row = partition.next()
      println(line.getInt(6) + " " + line.getString(7))
    }
  })

  dfParquetFile.show()

  /**
   * org.apache.spark.sql.Dataset (Class)
   * Functions:
   * as, columns, createOrReplaceGlobalTempView, createOrReplaceTempView, dtypes,
   * printSchema, rdd, schema
   */

  dfParquetFile.columns.foreach(println)

  dfParquetFile.as("newdf").select(col("newdf.OrderID")).show()

  dfParquetFile.createOrReplaceGlobalTempView("SalesGlobal")

  spark.sql("select * from spark_global.SalesGlobal limit 10").show()

  // Filter data for Asia region and register it as temp table
  dfParquetFile.filter(col("region") === "Asia").createOrReplaceTempView("SalesLocal")

  spark.sql("select * from SalesLocal limit 10").show()

  // Get all column names and their datatypes in an Array
  dfParquetFile.dtypes.foreach(println)

  // Convert dataframe to rdd
  val dfToRdd: RDD[Row] = dfParquetFile.rdd

  dfToRdd.take(5).foreach(println)










}
