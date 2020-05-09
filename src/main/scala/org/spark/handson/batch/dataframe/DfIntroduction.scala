package org.spark.handson.batch.dataframe

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode}
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
    .option("header", "true")
    .option("inferschema", "true")
    .csv("src/main/resources/input/csv/*.csv")

  dfCSVFile.show()

  // spark.read.json()
  val dfJSONFileSingleLine = spark.read.json("src/main/resources/input/json/singleline.json")
  dfJSONFileSingleLine.show(20, false)

  val dfJSONFileMultiLine =
    spark.read.option("multiline", true).json("src/main/resources/input/json/multiline.json")
  dfJSONFileMultiLine.show(20, false)

  // spark.read.orc()
  val dfORCFile = spark.read.orc("src/main/resources/input/orc")
  dfORCFile.show(20, false)

  // spark.read.parquet()
  val dfParquetFile = spark.read.parquet("src/main/resources/input/parquet")
  dfParquetFile.show(20, false)

  // spark.read.format
  val dfAvroFile = spark.read.format("avro").load("src/main/resources/input/avro")
  dfAvroFile.show(20, false)

  // Read data from MySql table
  val dfReadMySql = spark.read.jdbc(url, table = "Sales", dbConnection)
  dfReadMySql.show(10, false)

  // Write data to My SQL table
  dfAvroFile.write.mode(SaveMode.Overwrite).jdbc(url, table = "Sales", dbConnection)

  /**
   * Note: Currently bucketBy and SortBy is not supported with save method.
   *       It is only supported with saveAsTable which is saves data to hive.
   *       Please refer below JIRA for more details
   *
   * https://issues.apache.org/jira/browse/SPARK-19256
   *
   */
  dfAvroFile.write
    .partitionBy("country")
    //.bucketBy(5, "orderId")
    //.sortBy("orderId")
    .csv("src/main/resources/output/finaloutput/partition_bucket")




}
