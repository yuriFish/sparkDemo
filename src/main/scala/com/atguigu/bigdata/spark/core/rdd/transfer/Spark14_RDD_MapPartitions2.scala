package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_MapPartitions2 {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    mapPartitionsWithIndex 获取每个分区最大值和分区号

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 2, 5, 3, 6), 3)

    val rdd: RDD[(Int, Int)] = dataRDD.mapPartitionsWithIndex(
      (index, iter) => {
        List((index, iter.max)).iterator
      }
    )

    println(rdd.collect().mkString(","))

    sc.stop()
  }

}
