package org.spark.handson.rdd

import org.apache.spark.storage.StorageLevel
import org.spark.handson.utilities.SparkSessionConnection

object RddPersistence extends App with SparkSessionConnection {

  /**
   * We will target below RDD Persistence Operations
   *
   * 1. cache
   * 2. persist
   * 3. unpersist
   * 4. getStorageLevel
   *
   */

  // cache and persist
  /**
   * Caching RDDs in Spark: It is one mechanism to speed up applications that access the same RDD multiple times.
   * An RDD that is not cached, nor checkpointed, is re-evaluated again each time an action is invoked on that RDD.
   * There are two function calls for caching an RDD: cache() and persist(level: StorageLevel).
   * The difference among them is that cache() will cache the RDD into memory,
   * whereas persist(level) can cache in memory, on disk, or off-heap memory according to the caching
   * strategy specified by level. persist() without an argument is equivalent with cache().
   *
   */

  val rddSales = spark.read
    .option("header","true")
    .option("inferschema","true")
    .csv("src/main/resources/input/csv/*.csv").rdd

  rddSales.take(10).foreach(println)

  rddSales.cache()

  rddSales.unpersist(true)

  rddSales.persist(StorageLevel.DISK_ONLY)
  rddSales.unpersist(true)

  /**
   * Persistence levels are:
   * StorageLevel.DISK_ONLY_2
   * StorageLevel.MEMORY_AND_DISK
   * StorageLevel.MEMORY_AND_DISK_2
   * StorageLevel.MEMORY_AND_DISK_SER
   * StorageLevel.MEMORY_AND_DISK_SER_2
   * StorageLevel.MEMORY_ONLY_SER
   * StorageLevel.MEMORY_ONLY_SER_2
   * StorageLevel.MEMORY_ONLY
   * StorageLevel.MEMORY_ONLY_2
   * StorageLevel.OFF_HEAP
   */


  rddSales.persist(StorageLevel.MEMORY_ONLY)

  println(rddSales.count)
  println(rddSales.toDebugString)
  println(rddSales.getStorageLevel)

  rddSales.unpersist(true)

  println(rddSales.count)
  println(rddSales.toDebugString)

}
