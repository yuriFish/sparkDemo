package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark51_RDD_Persist {

  def main(args: Array[String]): Unit = {

    // TODO Spark cache缓存
    //  cache操作会增加血缘关系，不改变原有的血缘关系

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val mapRDD: RDD[(Int, Int)] = rdd.map(
      num => {
        println("map...")
        (num, 1)
      }
    )

    // 数据缓存。
//    mapRDD.cache()

    // 可以更改存储级别 以下后面都可以加"_2"
    // MEMORY_ONLY
    // MEMORY_ONLY_SER
    // MEMORY_AND_DISK
    // MEMORY_AND_DISK_SER
    // DISK_ONLY
    val cacheRDD: mapRDD.type = mapRDD.persist(StorageLevel.MEMORY_AND_DISK_2)
    println(cacheRDD.toDebugString)
    println(cacheRDD.collect().mkString("&"))
    println(cacheRDD.toDebugString)

    println(mapRDD.collect().mkString(","))
    println("---------")
    mapRDD.saveAsTextFile("output/save_persist")

    sc.stop()
  }

}
