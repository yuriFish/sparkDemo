package com.atguigu.bigdata.spark.core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark49_RDD_Dep01 {

  def main(args: Array[String]): Unit = {

    // TODO Spark依赖关系

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
    println("rdd.toDebugString = " + rdd.toDebugString)
    println("----------------------")

    val wordRDD: RDD[String] = rdd.flatMap(
      string => {
        string.split(" ")
      }
    )
    println("wordRDD.toDebugString = " + wordRDD.toDebugString)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println("mapRDD.toDebugString = " + mapRDD.toDebugString)
    println("----------------------")

    // 如果Spark的计算过程中某一个节点计算失败，那么框架会尝试重新计算
    // Spark既然想重新计算，那么就需要知道数据的来源，并且还需要知道数据经历了哪些计算
    // RDD不保存计算的数据，但是会保存元数据信息(从头开始计算，恢复丢失的数据)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println("reduceRDD.toDebugString = " + reduceRDD.toDebugString)
    println("----------------------")

    println(reduceRDD.collect().mkString(","))

    sc.stop()
  }

}
