package org.spark.handson.batch.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DfIntroduction extends App {

  // Create empty Dataframe
  val emptyDf = spark.emptyDataFrame


  // Create dataframe from rdd
  val rddData: RDD[(Char, Int)] =
    spark.sparkContext.parallelize(Seq(('A', 10), ('B', 20), ('C', 30), ('D', 40), ('E', 50)))
  val rddRow: RDD[Row] = rddData.map(line => Row.fromTuple(line._1.toString, line._2))

  val schema: StructType = StructType(StructField("name", StringType, false) ::
    StructField("age", IntegerType, false) :: Nil)

  val dfFromRdd = spark.createDataFrame(rddRow, schema)

  dfFromRdd.show()


  // Read data from different file formats
  // spark.read.textFile(<>)
  val dfText1File: Dataset[String] = spark.read.textFile("src/main/resources/input/txt/SampleData1.txt")
  dfText1File.show()

  // spark.read.textFile(<>,<>,<>)
  val dfText2File: Dataset[String] = spark.read.
    textFile("src/main/resources/input/txt/SampleData1.txt",
      "src/main/resources/input/txt/SampleData2.txt")
  dfText2File.show()

  // spark.read.textFile(<dir/*>)
  val dfTextAllFiles: Dataset[String] = spark.read.textFile("src/main/resources/input/txt/*.txt")
  dfTextAllFiles.show()

  // spark.read.csv()
  val dfCSVFile = spark.read
    .option("header","true")
    .option("inferschema","true")
    .csv("src/main/resources/input/csv/*.csv")

  dfCSVFile.show()


}
