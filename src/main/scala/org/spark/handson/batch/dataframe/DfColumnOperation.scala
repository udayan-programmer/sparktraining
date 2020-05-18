package org.spark.handson.batch.dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object DfColumnOperation extends App {
  /**
   * org.apache.spark.sql.Column
   * String Functions:
   *
   */

  // Read parquet file to do further operations
  val dfParquetFile = spark.read.parquet("src/main/resources/input/parquet")
  dfParquetFile.show()

  val filterResult = dfParquetFile.filter(
    col("ItemType") === "Clothes"
      && col("UnitsSold") >= 4000
      && col("UnitPrice").between(50, 1000)
      && col("Region").isin("Europe", "Asia", "North America")
      && col("TotalProfit").isNotNull
  )


  // Example of Cast
  val cols = filterResult.columns

  val lstToCast = List("UnitPrice","UnitCost","TotalRevenue","TotalCost")

  val res = cols.map(x => if (lstToCast.contains(x)) col(x).cast(IntegerType) else col(x))

  filterResult.select(res:_*).show(20, false)

  logger.info(s"Count of filter dataframe ${filterResult.count()}")


  // Few string functions on columns
  dfParquetFile.show(20,false)

  val stringOperationsDf = dfParquetFile
    .select(
      col("Region"),
      lower(col("Region")).as("region_lower"),
      length(col("Region")).as("region_length"),
      concat(col("Region"),lit("        ")).as("region_adding_spaces"),
      ltrim(concat(col("Region"),lit("        "))).as("region_left_trim"),
      rtrim(concat(col("Region"),lit("        "))).as("region_right_trim"),
      split(col("Region")," ").as("region_split"),
      upper(col("Region")).as("region_to_upper"),
      substring(col("Country"),0,3).as("country_substring")
    )

  stringOperationsDf.show(20, false)
  stringOperationsDf.printSchema()





}
