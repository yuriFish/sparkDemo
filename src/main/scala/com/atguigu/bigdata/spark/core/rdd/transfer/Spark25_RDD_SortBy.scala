package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_RDD_SortBy {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)
    //    sortBy 该操作用于排序数据。在排序之前，可以将数据通过f函数进行处理，之后按照f函数处理的结果进行排序，
    //            默认为正序排列。排序后新产生的RDD的分区数与原RDD的分区数一致。

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 2, 7, 5, 3, 6), 1)

    // TODO 默认true升序
    // sortBy 可以通过传递第二个参数改变排序的方式
    // sortBy 可以设定第三个参数改变分区
    val sortRDD = dataRDD.sortBy(num => num, false)
    println(sortRDD.collect().mkString(","))

    sc.stop()
  }

}
