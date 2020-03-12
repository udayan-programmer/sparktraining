package org.spark.handson.rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.spark.handson.utilities.SparkSessionConnection

object RddSharedVariables extends App with SparkSessionConnection{

  val rddTextFile: RDD[String] = spark.sparkContext
    .textFile(path = "src/main/resources/input/txt/SampleData1.txt", minPartitions = 4)
  rddTextFile.foreach(println)

  val broadcastRdd: Broadcast[Array[String]] = spark.sparkContext.broadcast(rddTextFile.collect)

  broadcastRdd.value.foreach(println)

  broadcastRdd.unpersist()

  val accumulator = spark.sparkContext.longAccumulator("My Accumulator")

  for (counter <- 1 to 5) {
    accumulator.add(counter)
  }

  println("Value of accumulator is: " + accumulator.value)


}