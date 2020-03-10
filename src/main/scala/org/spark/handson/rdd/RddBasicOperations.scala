package org.spark.handson.rdd

import org.apache.spark.rdd.RDD
import org.spark.handson.utilities.SparkSessionConnection

import scala.util.Random

object RddBasicOperations extends App with SparkSessionConnection {

  /**
   * We will target below basic operations on RDD here:
   * 1. isEmpty
   * 2. first
   * 3. filter
   * 4. take
   * 5. takeOrdered
   * 6. sample
   * 7. count
   * 8. collect
   * 9. map
   * 10.flatMap
   * 11.groupBy
   * 12.reduce
   * 13.saveAsTextFile
   *
   * Note: Please refer org.apache.spark.rdd.RDD class for more details
   */

  /**
   * Lets create few RDDs to apply above functions
   */

  // Empty RDD
  val rddEmpty: RDD[Nothing] = spark.sparkContext.emptyRDD

  // RDD with numbers
  val rddNumbers: RDD[Int] = spark.sparkContext.parallelize(Seq.concat(1 to 1000,
    500 to 1000,
    800 to 1500))

  // RDD with 10 numbers
  val rddNum: RDD[Int] = spark.sparkContext.parallelize(1 to 10)

  // RDD with words
  val rddString: RDD[String] =
    spark.sparkContext.parallelize(Seq("An", "Apple", "a", "day", "keeps", "a", "doctor", "away"))

  // Generate random elements for Sequence
  val random = Random
  val randomSeq: Seq[Float] = for {
    i <- 1 to random.nextInt(100)
  } yield i * random.nextFloat

  // RDD with randomly generated elements
  val rddRandom: RDD[Float] = spark.sparkContext.parallelize(randomSeq)

  // RDD with text file
  val rddTextFile: RDD[String] = spark.sparkContext
    .textFile(path = "/Users/udayan/datasets/SampleData1.txt", minPartitions = 4)


  /**
   * 1. def isEmpty(): Boolean
   */
  logger.info("rddEmpty.isEmpty()")
  println("Does RDD is empty? : " + rddEmpty.isEmpty())

  logger.info("rddNumbers.isEmpty()")
  println("Does RDD is empty? : " + rddNumbers.isEmpty())

  /**
   * 2. def first(): T
   */
  logger.info("rddNumbers.first()")
  println("First Element: " + rddNumbers.first())

  /**
   * 3. def filter(f: (T) ⇒ Boolean): RDD[T]
   */
  //Get all even numbers from list
  logger.info("Filter data to get even numbers")
  rddNumbers.filter(_ % 2 == 0).foreach(println)

  /**
   * 4. def take(num: Int): Array[T]
   */
  logger.info("Take any 50 elements from RDD")
  rddRandom.take(50).foreach(println)

  /**
   * 5. def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
   */
  logger.info("Take any 50 elements from RDD in order")
  rddRandom.takeOrdered(50).foreach(println)

  /**
   * 6. def sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong): RDD[T]
   * Description: Function to get sample data from your RDD
   * Here we are taking 10% data as sample
   */
  logger.info("Take sample data from RDD")
  rddNumbers.sample(false, 0.1, 0).foreach(println)

  /**
   * 7. def count(): Long
   */
  logger.info("Get number of records present in RDD")
  println(rddNumbers.count())

  /**
   * 8. i. def collect[U](f: PartialFunction[T, U]): RDD[U]
   *   ii. def collect(): Array[T]
   */
  logger.info("def collect[U](f: PartialFunction[T, U]): RDD[U]")
  val collectFuncRdd: RDD[Int] = rddNumbers.collect(11 to 20)

  logger.info("def collect(): Array[T]")
  val collect: Array[Int] = rddNumbers.collect()

  collectFuncRdd.foreach(println)
  println(collect.size)

  /**
   * 9. def map[U](f: (T) ⇒ U): RDD[U]
   *
   */
  logger.info("def map[U](f: (T) ⇒ U): RDD[U]")
  // Get each number from rddNumbers RDD and add 10 to it and finally display
  rddNum.map(_ + 10).foreach(println)

  //Change each number in rddNumbers. Return square of even numbers and cube of odd numbers along with number
  rddNum.map(num => (num, {
    if (num % 2 == 0) {
      num * num
    }
    else {
      num * num * num
    }
  })).foreach(println)

  /**
   * 10. def flatMap[U](f: (T) ⇒ TraversableOnce[U]): RDD[U]
   */
  logger.info("def flatMap[U](f: (T) ⇒ TraversableOnce[U]): RDD[U]")
  // Behaviour of map on multiline text RDD
  val mapTextRdd: RDD[Array[String]] = rddTextFile.map(_.split((" ")))
  /*
      mapTextRdd.collect()
      res13: Array[Array[String]] = Array(Array(I, am, inside, file, SampleData1.txt), Array(Good, Morning),
      Array(Good, Evening), Array(End, of, File, SampleData1.txt))
   */

  // Behaviour of flatMap on multiline text RDD
  val flatMapTextRdd: RDD[String] = rddTextFile.flatMap(_.split(" "))
  /*
      flatMapTextRdd.collect()
      res17: Array[String] = Array(I, am, inside, file, SampleData1.txt, Good, Morning, Good, Evening, End, of,
      File, SampleData1.txt)
   */

  /**
   * 11. def groupBy[K](f: (T) ⇒ K): RDD[(K, Iterable[T])]
   * def groupBy[K](f: (T) ⇒ K, numPartitions: Int): RDD[(K, Iterable[T])]
   */
  // Group the numbers based on even and odd type and save it in 2 partitions
  logger.info("def groupBy[K](f: (T) ⇒ K, numPartitions: Int): RDD[(K, Iterable[T])]")
  val rddOddEvenGroup: RDD[(String, Iterable[Int])] = rddNum
    .groupBy(num => if (num % 2 == 0) "Even" else "Odd", numPartitions = 2)
  println(s"Number of partitions for RDD Odd Even Group = ${rddOddEvenGroup.getNumPartitions}")

  val rddOddEvenGroupFinal: RDD[(String, Seq[Int])] = rddOddEvenGroup
    .map(element => (element._1, element._2.toSeq.sortBy(x => x)))
  rddOddEvenGroupFinal.foreach(data => println(s"${data._1}, ${data._2}"))
  println(rddOddEvenGroupFinal.getNumPartitions)

  // Group the words based on its 1st characters
  // Here we are changing our word to lowercase and then grouping them
  logger.info("def groupBy[K](f: (T) ⇒ K): RDD[(K, Iterable[T])]")
  val rddFirstGroup: RDD[(Char, Seq[String])] =
    rddString.groupBy(word => word.charAt(0).toLower)
      .map(data => (data._1, data._2.toSeq.map(word => word.toLowerCase).sortBy(x => x.toLowerCase)))
  rddFirstGroup.foreach(data => println(s"${data._1}, ${data._2}"))

  /**
   * 12. def reduce(f: (T, T) ⇒ T): T
   *
   * Description:
   * List( 1, 2, 3, 4 ).reduce( (x,y) => x + y )
   * Step 1 : op( 1, 2 ) will be the first evaluation.
   * Start with 1, 2, that is
   * x is 1  and  y is 2
   * Step 2:  op( op( 1, 2 ), 3 ) - take the next element 3
   * Take the next element 3:
   * x is op(1,2) = 3   and y = 3
   * Step 3:  op( op( op( 1, 2 ), 3 ), 4)
   * Take the next element 4:
   * x is op(op(1,2), 3 ) = op( 3,3 ) = 6    and y is 4
   *
   * Result here is the sum of the list elements, 10.
   * op( op( ... op(x_1, x_2) ..., x_{n-1}), x_n)
   */
  println(rddNum.reduce((x, y) => x + y))
  println(rddNum.reduce((x, y) => x min y))
  println(rddNum.reduce((x, y) => x max y))

  /**
   * 13. def saveAsTextFile(path: String): Unit
   * def saveAsTextFile(path: String, codec: Class[_ <:CompressionCodec]): Unit
   *
   */
  // rddTextFile.saveAsTextFile("/Users/udayan/datasets/output/nocompression")
  // rddTextFile.saveAsTextFile("/Users/udayan/datasets/output/gzip/", classOf[org.apache.hadoop.io.compress.GzipCodec])
}
