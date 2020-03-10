package org.spark.handson.rdd

import org.apache.spark.rdd.RDD
import org.spark.handson.utilities.SparkSessionConnection

object RddPartitionOperations extends App with SparkSessionConnection {

  /**
   * We will target below partition operation on RDD here:
   *  1. partitions
   *  2. getNumPartition
   *  3. repartition
   *  4. mapPartitions
   *  5. coalesce
   *  6. mapPartitionsWithIndex
   *  7. foreachPartition
   *  8. zipPartitions
   *  9. zipWithIndex
   */

  val rddNumbers: RDD[Int] =
    spark.sparkContext.parallelize(Seq.concat(1 to 1000,
      500 to 1000,
      800 to 1500,
      for (i <- 1 to 10000) yield 1
    ), 4)


  /**
   * 1. partitions
   */
  rddNumbers.partitions.foreach(println)

  /**
   * 2. getNumPartition
   */
  println(rddNumbers.getNumPartitions)

  /**
   * 3. repartition
   */
  println(rddNumbers.repartition(20).getNumPartitions)


  /**
   * 4. mapPartition
   * Description:
   * mapPartitions() can be used as an alternative to map() & foreach().
   * mapPartitions() is called once for each Partition unlike map() & foreach() which is called for each element
   * in the RDD.
   * The main advantage being that, we can do initialization on Per-Partition basis instead of
   * per-element basis(as done by map() & foreach())
   *
   * Consider the case of Initializing a database.
   * If we are using map() or foreach(), the number of times we would need to initialize will be equal to
   * the no of elements in RDD. Whereas if we use mapPartitions(), the no of times we would need to
   * initialize would be equal to number of Partitions
   */

  println("Initial Partition Size of RDD: " + rddNumbers.getNumPartitions.toString)
  val mapPartition1 = rddNumbers.mapPartitions(x => List(x.size).iterator).collect
  mapPartition1.foreach(println)

  val mapPartitionRdd = rddNumbers.repartition(10)
  println("Total partitions available for RDD after repartition: " + mapPartitionRdd.getNumPartitions)

  val mapPartitionResult = mapPartitionRdd.mapPartitions(x => List(x.size).iterator).collect
  mapPartitionResult.foreach(println)


  /**
   * 5. coalesce:
   * Initially we have 4 partitions on rddNumbers RDD.
   *
   * Test Case 1:
   * coalesce(numPartitions = 10, shuffle = false)
   *
   * Test Case 2:
   * coalesce(numPartitions = 2, shuffle = false)
   *
   * Test Case 3:
   * coalesce(numPartitions = 10, shuffle = true)
   *
   * Note:
   * With shuffle = true, you can actually coalesce to a larger number
   * of partitions. This is useful if you have a small number of partitions,
   * say 100, potentially with a few partitions being abnormally large. Calling
   * coalesce(1000, shuffle = true) will result in 1000 partitions with the
   * data distributed using a hash partitioner.
   *
   * Note:
   * When we use coalesce to decrease number of partitions, our partitions will be created
   * with unequal size.
   */


  // Test Case 1: Initial Partitions - 4
  println("Initial Partition Size of RDD: " + rddNumbers.getNumPartitions.toString)
  mapPartition1.foreach(println)
  println(rddNumbers.coalesce(10, false).getNumPartitions)
  // Output: Partitions = 4 ----->> Partitions not increased here due to shuffle property is false

  // Test Case 2: Initial Partitions - 4
  val coalesceRdd1 = rddNumbers.coalesce(3, false)
  println(coalesceRdd1.getNumPartitions) // Output: Partitions = 3
  coalesceRdd1.mapPartitions(x => List(x.size).iterator).foreach(println)
  /*  Here we can see uneven distribution of data
      6101
      3051
      3050
   */

  // Test Case 3: Initial Partition - 3
  val coalesceRdd2 = coalesceRdd1.coalesce(10, true)
  println(coalesceRdd2.getNumPartitions)
  // Output: Partitions = 10 with equal distribution
  coalesceRdd2.mapPartitions(x => List(x.size).iterator).foreach(println)

  /**
   * 6. mapPartitionsWithIndex
   *
   */

  // Get Index number for each element in RDD
  val distDataIndex: RDD[(Int, Int)] = rddNumbers
    .mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => it.toList.map(x => (index, x)).iterator)
  distDataIndex.foreach(println)

  // Print data from partition 3 only in sorted order
  distDataIndex.sortByKey().collect().foreach {
    case (key, value) => {
      if (key == 3) println(s"($key,$value)")
    }
  }

  /**
   *
   * @param index
   * @param iter
   * @tparam A
   * @tparam B
   * @return Returns filtered data as Iterator[(A,B)]
   */
  def filterPartition[A, B](index: A, iter: Iterator[B]): Iterator[(A, B)] = {
    val lst: List[(A, B)] = iter.toList.map(x => (index, x))
    lst.filter(_._1 == partition).iterator
  }

  // Filter data from RDD of Int or String type and get values from partition 4
  val partition: Any = 2

  // Filter data of Int type
  val testRDD = rddNumbers.mapPartitionsWithIndex(filterPartition)
  testRDD.foreach(println)

  // Filter data of String type
  val stringRDD = spark.
    sparkContext.parallelize(Seq("Apple", "Grapes", "Mango", "Guava", "Pears", "Pineapple"), 3)

  val testStrRDD = stringRDD.mapPartitionsWithIndex(filterPartition)
  testStrRDD.foreach(println)

  /**
   * 7. foreachPartition
   *
   */
  stringRDD.foreachPartition(iter => {
    println("Entering Partition.......")
    iter.foreach(println)
  })

  /**
   * 8. zipPartitions
   *
   * T => rddNumber1
   * B => rddNumber2
   * V => Result
   *
   * def zipPartitions[B: ClassTag, V: ClassTag]
   * (rdd2: RDD[B], preservesPartitioning: Boolean)
   * (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V]
   *
   */
  // Define 2 Rdds with 3 partitions each
  val rddNumber1: RDD[Int] = spark.sparkContext.parallelize(1 to 10, 3)
  val rddNumber2: RDD[Int] = spark.sparkContext.parallelize(11 to 20, 3)

  // Get details of records in each partition
  rddNumber1
    .mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => it.toList.map(x => (index, x)).iterator)
    .sortBy(_._1, true).collect.foreach(x => print(s" $x "))
  // Output: (0,1)  (0,2)  (0,3)
  //         (1,4)  (1,5)  (1,6)
  //         (2,7)  (2,8)  (2,9)  (2,10)

  rddNumber2
    .mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => it.toList.map(x => (index, x)).iterator)
    .sortBy(_._1, true).collect.foreach(x => print(s" $x "))
  // Output: (0,11)  (0,12)  (0,13)  (0,14)  (0,15)
  //         (1,16)  (1,17)  (1,18)  (1,19)  (1,20)
  //         (2,21)  (2,22)  (2,23)  (2,24)  (2,25)

  // Function to perform operation on zipPartitions
  def zipMyRdds(iter1: Iterator[Int], iter2: Iterator[Int]): Iterator[(Int, Int)] = {
    val lst1: List[Int] = iter1.toList
    val lst2: List[Int] = iter2.toList

    val result: List[(Int, Int)] = for {
      num1 <- lst1
      num2 <- lst2
    } yield (num1, num2)
    result.toIterator
  }

  // Make call to zipPartitions
  val rddNumber3: RDD[(Int, Int)] = rddNumber1.zipPartitions(rddNumber2, true)(zipMyRdds)
  rddNumber3.sortByKey(true).collect.foreach(x => print(s" $x "))
  // Output:
  // Partition 0:  (1,11)  (1,12)  (1,13)  (1,14)  (1,15)
  //               (2,11)  (2,12)  (2,13)  (2,14)  (2,15)
  //               (3,11)  (3,12)  (3,13)  (3,14)  (3,15)

  // Partition 1:  (4,16)  (4,17)  (4,18)  (4,19)  (4,20)
  //               (5,16)  (5,17)  (5,18)  (5,19)  (5,20)
  //               (6,16)  (6,17)  (6,18)  (6,19)  (6,20)

  // Partition 2:  (7,21)  (7,22)  (7,23)  (7,24)  (7,25)
  //               (8,21)  (8,22)  (8,23)  (8,24)  (8,25)
  //               (9,21)  (9,22)  (9,23)  (9,24)  (9,25)
  //               (10,21)  (10,22)  (10,23)  (10,24)  (10,25)

  /**
   * 9. zipWithIndex
   * def zipWithIndex(): RDD[(T, Long)]
   *
   * Zips this RDD with its element indices. The ordering is first based on the partition index
   * and then the ordering of items within each partition. So the first item in the first
   * partition gets index 0, and the last item in the last partition receives the largest index.
   */
  val rddZipWithIndex: RDD[(Int, Long)] = rddNumber2.zipWithIndex()
  rddZipWithIndex.sortByKey().collect().foreach(x => print(s" $x "))
  // Output:  (11,0)  (12,1)  (13,2)  (14,3)  (15,4)  (16,5)  (17,6)  (18,7)  (19,8)  (20,9)  (21,10)
  //          (22,11)  (23,12)  (24,13)  (25,14)


}
