package org.spark.handson.rdd

import org.apache.spark.rdd.RDD

object RddIntroduction extends App {

  /**
   * Possible ways to create RDD.
   *    1. Create Empty RDD
   *    2. Using parallelize method
   *    3. Using existing data file in filesystem
   *
   */

  /**
   * Create Empty RDD
   */
  // Create empty RDD of Nothing Type
  val rdd: RDD[Nothing] = spark.sparkContext.emptyRDD
  println(rdd)
  rdd.getNumPartitions

  // Create empty RDD of String Type
  val rddString = spark.sparkContext.emptyRDD[String]
  println(rddString)
  rddString.getNumPartitions

  // Create empty RDD of String Type with Partitions
  val rddWithPartition = spark.sparkContext.parallelize(Seq.empty[String])
  println(rddWithPartition)
  rddWithPartition.getNumPartitions

  // Create empty PairRDD
  type pairRDDType = (String, Int)
  val emptyPairRDD = spark.sparkContext.parallelize(Seq.empty[pairRDDType])
  println(emptyPairRDD)
  emptyPairRDD.getNumPartitions

  /**
   * Using parallelize method
   */
  // RDD with Sequence of Numbers
  val rddSeq: RDD[Int] =
  spark.sparkContext.parallelize(Seq.concat(1 to 1000,
    500 to 1000,
    800 to 1500))
  rddSeq.take(10).foreach(println)

  // Sequence of Strings
  val rddWords =
    spark.sparkContext.parallelize(Seq("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"))
  val wordPair = rddWords.map(w => (w.charAt(0), w))
  wordPair.foreach(println)

  // Sequence of Tuples i.e. Create PairRDD
  val pairRDD: RDD[(Int, String)] = spark.sparkContext.parallelize(Seq((1, "Mango"), (2, "Orange"), (3, "Apple"),
    (4, "Grapes"), (2, "Orange"), (1, "Mango")))
  pairRDD.foreach(println)

  // RDD with Case Classes (Only available with Spark Scala)
  case class Person(name: String, gender: Char, age: Int)

  val personData: Seq[Person] = Seq(
    Person("Anand", 'M', 30),
    Person("Vishal", 'M', 31),
    Person("Ankita", 'F', 22)
  )
  val rddPerson: RDD[Person] = spark.sparkContext.parallelize(personData)
  rddPerson.foreach {
    person =>
      println(s"Name: ${person.name}, Gender: ${person.gender}, Age: ${person.age}")
  }

  /**
   * Using existing data file in filesystem
   */
  val rddTextFile: RDD[String] = spark.sparkContext
    .textFile(path = "src/main/resources/input/txt/SampleData1.txt", minPartitions = 4)
  rddTextFile.foreach(println)
}
