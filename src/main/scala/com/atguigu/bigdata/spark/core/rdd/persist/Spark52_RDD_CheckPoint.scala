package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark52_RDD_CheckPoint {

  def main(args: Array[String]): Unit = {
    //  TODO Spark cache缓存
    //  cache操作会增加血缘关系，不改变原有的血缘关系
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("checkpoint/")

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val mapRDD: RDD[(Int, Int)] = rdd.map(
      num => {
        println("map...")
        (num, 1)
      }
    )

    // TODO 将比较耗时，比较重要的数据一般会保存到分布式文件系统中
    //      使用CheckPoint方法将数据保存到文件
    //  SparkException: Checkpoint directory has not been set in the SparkContext
    //  执行checkpoint方法前应该设定检查点的保存目录 sc.setCheckpointDir("dirName/")
    //    检查点的操作中为了保证数据的准确性，会执行时，会启动新的job
    //    为了提高性能，检查点操作一般会和cache联合使用
    val cacheRDD: mapRDD.type = mapRDD.persist(StorageLevel.MEMORY_AND_DISK_2)
//    val unit: Unit = mapRDD.checkpoint()

    // TODO 检查点操作会切断血缘关系，一旦数据丢失不会重头读取数据
    //      因为检查点会将数据保存到分布式存储系统中，数据相对来说比较安全，不容易丢失
    //      所以会切断血缘，等同于产生新的数据源
    mapRDD.checkpoint()
//    println(mapRDD.toDebugString)
//    println(mapRDD.collect().mkString("&"))
//    println(mapRDD.toDebugString)

//    println(mapRDD.collect().mkString(","))
//    println("---------")
//    println(mapRDD.collect().mkString("&"))

    println(cacheRDD.collect().mkString(","))
    println("---------")
    println(cacheRDD.collect().mkString("&"))

    sc.stop()
  }

}
