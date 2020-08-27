package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_MapPartitions1 {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //      map 每一次处理一条数据
    //      mapPartitions 每一次将一个分区的数据当成一个整体进行数据处理
    //                    如果一个分区的数据没有处理完，那么所有的数据都不会被释放，即使前面已经处理完的数据也不会被释放，容易出现内存溢出

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

//    val rdd: RDD[Int] = dataRDD.mapPartitions(
//      iter => {
//        iter.map(_*2)
//      }
//    )
//
//    println(rdd.collect.mkString(","))

//    val rdd: RDD[Int] = dataRDD.mapPartitions(
//      iter => {
//        iter.filter(_ % 2 == 0)
//      }
//    )
//
//    println(rdd.collect.mkString(","))

    val dataRDD2: RDD[Int] = sc.makeRDD(List(1, 4, 2, 5, 3, 6), 2)

//    val rdd1: RDD[Int] = dataRDD2.mapPartitions(
//      iter => {
//        List(iter.max).iterator
//      }
//    )
//
//    println(rdd1.collect.mkString(","))


    sc.stop()
  }

}
