package org.spark.handson.batch.rdd

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD


object PairRddOperationsOnKey extends App {

  /**
   * Here we will target below Key operations on Pair RDD.
   *
   *  1. keys
   *  2. join
   *  3. leftOuterJoin
   *  4. rightOuterJoin
   *  5. fullOuterJoin
   *  6. cogroup
   *  7. countByKey
   *  8. countApproxDistinctByKey
   *  9. groupByKey
   *  10.reduceByKey
   *  11.subtractByKey
   *  12.lookup
   *  13.partitionBy
   */

  /**
   * Lets create few RDDs that we will work on
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

  val pairRDD2: RDD[(String, Int)] = spark.sparkContext.parallelize(
    Seq(("Maths", 65),
      ("Science", 75),
      ("Hindi", 70),
      ("Computer Science", 98)
    ))

  /**
   * 1. keys: RDD[K] = self.map(_._1)
   */
  pairRDD1.keys.foreach(println)

  /**
   * 2. join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
   *
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  pairRDD1.join(pairRDD2).foreach(println)

  /**
   * 3. leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
   *
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  pairRDD1.leftOuterJoin(pairRDD2).foreach(println)

  /**
   * 4. rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], W))]
   *
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   *
   */
  pairRDD1.rightOuterJoin(pairRDD2).foreach(println)

  /**
   * 5. fullOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], Option[W]))]
   *
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDD.
   *
   */
  pairRDD1.fullOuterJoin(pairRDD2).foreach(println)

  /**
   * 6. cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W]))]
   *
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  val cogroupRddResult: RDD[(String, (List[Int], List[Int]))] = pairRDD1.cogroup(pairRDD2).map {
    data => {
      val key: String = data._1
      val value1: List[Int] = data._2._1.toList
      val value2: List[Int] = data._2._2.toList

      (key, (value1, value2))
    }
  }
  cogroupRddResult.foreach(println)

  /**
   * 7. countByKey(): Map[K, Long]
   * Count the number of elements for each key, collecting the results to a local Map.
   */
  val countByKeyResult: collection.Map[String, Long] = pairRDD1.countByKey()
  countByKeyResult.foreach(println)

  /**
   * 8. countApproxDistinctByKey(relativeSD: Double = 0.05): RDD[(K, Long)]
   * Return approximate number of distinct values for each key in this RDD.
   */
  pairRDD1.countApproxDistinctByKey().foreach(println)

  /**
   * 9. groupByKey(): RDD[(K, Iterable[V])]
   *
   * Group the values for each key in the RDD into a single sequence.
   */
  pairRDD1.groupByKey().foreach(println)

  /**
   * 10. reduceByKey(func: (V, V) => V): RDD[(K, V)]
   *
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   *
   */
  // Below code create union of 2 rdds and groups data on key and calculate sum of marks obtained in each subject
  pairRDD1.union(pairRDD2).reduceByKey(_ + _).foreach(println)

  /**
   * 11. subtractByKey[W: ClassTag](other: RDD[(K, W)]): RDD[(K, V)]
   *
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   */
  pairRDD1.subtractByKey(pairRDD2).foreach(println)

  /**
   * 12. lookup(key: K): Seq[V]
   *
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   *
   */
  pairRDD1.lookup("Maths").foreach(println)

  /**
   * 13. partitionBy
   *
   */
  println(pairRDD1.getNumPartitions)
  val partitionResult: RDD[(String, Int)] = pairRDD1.
    partitionBy(pairRDD1.partitioner.getOrElse(new HashPartitioner(3)))
  println(partitionResult.getNumPartitions)


}
