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

  val accRdd = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6))

  accRdd.map(x => accumulator.add(x))
  // Here value of accumulator will be 0 as map is not an action
  println("Value of accumulator is: " + accumulator.value)

  accRdd.foreach(x => accumulator.add(x))

  println("Value of accumulator is: " + accumulator.value)

  Thread.sleep(300000)

}
