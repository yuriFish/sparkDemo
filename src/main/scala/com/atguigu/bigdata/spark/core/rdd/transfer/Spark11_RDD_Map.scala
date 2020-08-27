package com.atguigu.bigdata.spark.core.rdd.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Map {

  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD - 算子(方法)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // TODO 分区内数据按照顺序依次执行
    //      分区间数据执行没有顺序，而且无需等待
    val rdd1: RDD[Int] = rdd.map(x => {
      println("map A = " + x)
      x
    })

    val rdd2: RDD[Int] = rdd1.map(x => {
      println("map B = " + x)
      x
    })

    println(rdd2.collect().mkString(","))

    sc.stop()
  }

}
