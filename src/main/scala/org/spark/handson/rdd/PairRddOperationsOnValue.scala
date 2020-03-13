package org.spark.handson.rdd

import java.io

import org.apache.spark.rdd.RDD


object PairRddOperationsOnValue extends App {

  /**
   * Here we will target below value operations on Rdd
   *
   * 1. values
   * 2. mapValues
   * 3. flatMapValues
   *
   */

  val pairRDD1: RDD[(String, Int)] = spark.sparkContext.parallelize(
    Seq(("Maths", 80),
      ("Science", 85),
      ("English", 90),
      ("Social Science", 83),
      ("Drawing & Painting", 100),
      ("Maths", 73),
      ("English", 99),
      ("Maths", 73)
    ))

  /**
   * 1. values
   *
   * Return an RDD with the values of each tuple.
   */
  pairRDD1.values.foreach(x => print(s" $x "))
  // Output
  // 100  73  90  83  99  80  85  73

  /**
   * 2. mapValues
   */
  pairRDD1.mapValues(value => value - 10).foreach(println)
  // Output
  // (Maths,70)
  // (Science,75)
  // (English,80)
  // (Social Science,73)
  // (Drawing & Painting,90)
  // (Maths,63)
  // (English,89)
  // (Maths,63)

  /**
   * 3. flatMapValues
   */
  val pairRDD2 = spark.sparkContext.parallelize(
    Seq(("Maths", 10), ("Science", 20)))

  pairRDD2.flatMapValues(x => (x + 10, x + 20).productIterator).foreach(println)
  // Output
  // (Science,30)
  // (Science,40)
  // (Maths,20)
  // (Maths,30)


}
