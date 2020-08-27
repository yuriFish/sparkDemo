package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_MapPartitionsTest {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    mapPartitionsWithIndex 获取每个分区最大值和分区号

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 获取第二个分区的数据
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 2, 5, 3, 6), 3)

    // 获取的分区索引从0开始
    val rdd: RDD[Int] = dataRDD.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )

    println(rdd.collect().mkString(","))

    sc.stop()
  }

}
