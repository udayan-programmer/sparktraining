package org.spark.handson.rdd

import org.apache.spark.rdd.RDD


object RddSetOperations extends App {

  /**
   * We will target below set operations on RDD here:
   * 1. ++
   * 2. intersection
   * 3. subtract
   * 4. union
   * 5. distinct
   * 6. zip
   * Note: ++ and zip are not set operations but we are covering it here.
   */

  /**
   * Lets create few RDDs to apply above functions
   */

  val rddSet1 = spark.sparkContext.textFile("src/main/resources/input/txt/SampleData1.txt")
    .repartition(1)
  println("Printing data for rddSet1")
  rddSet1.foreach(println)

  val rddSet2 = spark.sparkContext.textFile("src/main/resources/input/txt/SampleData2.txt")
    .repartition(1)
  println("Printing data for rddSet2")
  rddSet2.foreach(println)

  val rddSet3 = spark.sparkContext.textFile("src/main/resources/input/txt/SampleData3.txt")
    .repartition(1)
  println("Printing data for rddSet3")
  rddSet3.foreach(println)

  val rddNum1 = spark.sparkContext.parallelize(List(1, 2, 3, 4, 4, 4, 4))
  val rddNum2 = spark.sparkContext.parallelize(List(4, 5, 6, 6, 6, 6, 6))
  val rddNum3 = spark.sparkContext.parallelize(List(7, 8, 9, 10))

  /**
   * 1. def ++(other: RDD[T]): RDD[T]
   * Note: ++ do not eliminate duplicate data
   */
  val rddUnion: RDD[Int] = rddNum1.++(rddNum2)
  rddUnion.foreach(x => print(s" $x "))

  /**
   * 2. def intersection(other: RDD[T]): RDD[T]
   * def intersection(other: RDD[T], numPartitions: Int): RDD[T]
   */
  val rddIntersection: RDD[Int] = rddNum1.intersection(rddNum2, numPartitions = 1)
  rddIntersection.foreach(x => print(s" $x "))

  /**
   * 3. def subtract(other: RDD[T]): RDD[T]
   * def subtract(other: RDD[T], numPartitions: Int): RDD[T]
   */
  val rddSubtract: RDD[Int] = rddNum1.subtract(rddNum2, numPartitions = 1)
  rddSubtract.foreach(x => print(s" $x "))

  /**
   * 4. def union(other: RDD[T]): RDD[T]
   */
  val rddAllUnion: RDD[Int] = rddNum1.union(rddNum2).union(rddNum3)
  rddAllUnion.foreach(x => print(s" $x "))

  /**
   * 5. def distinct(): RDD[T]
   * def distinct(numPartitions: Int): RDD[T]
   */
  val rddDistinct = rddNum2.distinct(1)
  rddDistinct.foreach(x => print(s" $x "))

  /**
   * 6. def zip[U](other: RDD[U])(implicit arg0: ClassTag[U]): RDD[(T, U)]
   * Note: We can only zip RDDs with same number of elements in each partition
   */
  val rddZip = rddNum1.zip(rddNum2)
  rddZip.foreach(x => print(s" $x "))
  rddSet2.zip(rddSet3).foreach(println)
}
